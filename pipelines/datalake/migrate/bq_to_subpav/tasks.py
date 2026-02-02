# -*- coding: utf-8 -*-
# pylint: disable=import-error

"""
Tasks para migração de dados do BigQuery para o MySQL da SUBPAV.
"""
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from google.api_core.exceptions import GoogleAPIError
from google.cloud import bigquery
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

from pipelines.datalake.migrate.bq_to_subpav.utils import (
    _execute_batches,
    clean_str,
    default_metrics,
    ensure_dataframe_columns,
    extract_query_params,
    format_query,
    inject_db_schema_in_query,
    should_notify,
    summarize_error,
    validate_bq_columns,
    filter_exames_update_sintomatico
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import get_secret_key

DEFAULT_REPORT_CONTEXT = {
    "pipeline_name": "Migração BQ→MySQL SUBPAV",
    "monitor_slug": "subpav",
    "project": "",
    "notify": False,
    "environment": "dev",
}

DF_FILTERS = {
    "exames_update_sintomatico": filter_exames_update_sintomatico,
}

@task
def resolve_notify(project: str, notify_param) -> bool:
    """
    Valida necessidade de notificação do relatorio
    """
    return notify_param if notify_param is not None else should_notify(project)


def _safe_bq_column(column: str) -> str:
    """Remove caracteres potencialmente problemáticos em nomes/expressões de coluna."""
    return re.sub(r"[^\w\.\s]", "", column)


def _build_bq_select_query(
    project_id: str,
    dataset_id: str,
    table_id: str,
    bq_columns: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> tuple[str, str]:
    """Monta a query SELECT e retorna também o full_ref (para logs)."""
    full_ref = f"{project_id}.{dataset_id}.{table_id}"

    cols = validate_bq_columns(bq_columns)
    columns_clause = ", ".join(_safe_bq_column(c) for c in cols) if cols else "*"
    limit_clause = f"LIMIT {limit}" if limit else ""

    query = f"SELECT {columns_clause} FROM `{full_ref}` {limit_clause}"
    return query, full_ref


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def query_bq_table(
    dataset_id: str,
    table_id: str,
    environment: str,
    bq_columns: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Executa a consulta no BigQuery.
    """
    project_id = "rj-sms-dev" if environment.lower() == "dev" else "rj-sms"
    query, full_ref = _build_bq_select_query(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id,
        bq_columns=bq_columns,
        limit=limit,
    )

    try:
        client = bigquery.Client(project=project_id)
        df = client.query(query).to_dataframe()
        log(f"[{environment.upper()}] {full_ref} → {len(df)} registros", level="info")
        metrics = default_metrics()
        return {"df": df, "metrics": metrics}

    except GoogleAPIError as exc:
        msg = (
            f"[{environment.upper()}] Erro ao consultar BigQuery ({full_ref}): "
            f"{type(exc).__name__}: {exc}"
        )
        log(msg, level="error")

        metrics = default_metrics()
        metrics["errors"].append(msg)
        metrics["failed"] = metrics["total"]
        metrics["run_id"] = f"{table_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        metrics["execution_time"] = 0.0
        return {"df": None, "metrics": metrics}


@task
def get_db_uri(infisical_path: str, secret_name: str, environment: str = "dev") -> str:
    """
    Cria url de conexão Mysql usando a credencial do infisical
    """
    db_uri = get_secret_key.run(
        secret_path=infisical_path, secret_name=secret_name, environment=environment
    )
    if not db_uri:
        raise ValueError("DB_URI não encontrado. Verifique caminho, nome do secret e ambiente.")
    return db_uri


@task
def build_insert_config(db_schema: str, table_name: str, **kwargs: Any) -> Dict[str, Any]:
    """
    Monta o dicionário de configuração para inserção no MySQL.
    Aceita chaves opcionais via kwargs
    (ex.:
        if_exists,
        custom_insert_query,
        environment,
        batch_size
    ).
    """
    allowed = {"if_exists", "custom_insert_query", "environment", "batch_size"}
    unknown = set(kwargs) - allowed
    if unknown:
        raise ValueError(f"Chaves inválidas em config: {', '.join(sorted(unknown))}")

    config: Dict[str, Any] = {"db_schema": db_schema, "table_name": table_name}
    config.update(kwargs)
    return config


DEFAULT_INSERT_CONFIG: Dict[str, Any] = {
    "if_exists": "append",
    "custom_insert_query": None,
    "environment": "dev",
    "batch_size": 1000,
}


def _normalize_insert_config(config: Dict[str, Any]) -> Dict[str, Any]:
    required = {"db_schema", "table_name"}
    missing = required - set(config.keys())
    if missing:
        raise ValueError(f"Config inválido: faltando {', '.join(sorted(missing))}")

    # merge de defaults + user config
    return {**DEFAULT_INSERT_CONFIG, **config}


def _mark_empty_df_metrics(
    metrics: Dict[str, Any],
    environment: str,
    table_name: str,
    start: datetime,
) -> Dict[str, Any]:
    msg = f"[{environment.upper()}] sem registros para inserir em {table_name}"
    log(msg, level="info")
    metrics["notes"].append(msg)
    metrics["execution_time"] = (datetime.now() - start).total_seconds()
    return metrics


def _prepare_custom_insert(
    df: pd.DataFrame,
    custom_insert_query: str,
    db_schema: str,
    metrics: Dict[str, Any],
    start: datetime,
) -> tuple[Optional[pd.DataFrame], Optional[str]]:
    query = inject_db_schema_in_query(custom_insert_query, db_schema)

    if not format_query(query):
        msg = "❌ Query customizada fora do padrão permitido!"
        log(msg, level="error")
        metrics["errors"].append("Query customizada fora do padrão permitido.")
        metrics["failed"] = metrics["total"]
        metrics["execution_time"] = (datetime.now() - start).total_seconds()
        return None, None

    required_columns = extract_query_params(query)
    if not required_columns:
        msg = "Não foi possível identificar parâmetros obrigatórios na query customizada."
        log(msg, level="error")
        metrics["errors"].append(msg)
        metrics["failed"] = metrics["total"]
        metrics["execution_time"] = (datetime.now() - start).total_seconds()
        return None, None

    df_req = ensure_dataframe_columns(df, required_columns, fill_value=None, notes=metrics["notes"])

    campos_none = [col for col in required_columns if df_req[col].isnull().all()]
    if campos_none:
        warn_msg = (
            f"Atenção: as colunas {', '.join(campos_none)} "
            "foram preenchidas somente com None (cheque o schema de destino)."
        )
        log(warn_msg, level="warning")
        metrics["notes"].append(warn_msg)

    return df_req, query


def _run_custom_insert(
    conn,
    df_req: pd.DataFrame,
    query: str,
    batch_size: int,
    metrics: Dict[str, Any],
) -> None:
    recs = df_req.where(pd.notnull(df_req), None).to_dict(orient="records")
    ok, err, batch_errors = _execute_batches(conn, query, recs, batch_size)
    metrics["inserted"] = ok
    metrics["failed"] = err
    metrics["errors"].extend(batch_errors)


def _run_to_sql_insert(
    conn,
    df: pd.DataFrame,
    cfg: Dict[str, Any],
    metrics: Dict[str, Any],
) -> None:
    try:
        df.to_sql(
            name=cfg["table_name"],
            con=conn,
            schema=cfg["db_schema"],
            if_exists=cfg["if_exists"],
            index=False,
            chunksize=cfg["batch_size"],
        )
        metrics["inserted"] = metrics["total"]
    except (SQLAlchemyError, ValueError) as exc:
        msg = f"❌ Falha na inserção: {type(exc).__name__}: {exc}"
        log(msg, level="error")
        metrics["failed"] = metrics["total"]
        metrics["errors"].append(msg)


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def insert_df_into_mysql(
    df: pd.DataFrame,
    mysql_uri: str,
    config: Dict[str, Any],
    metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Responsável pela inserção de dados no Mysql.
    """
    cfg = _normalize_insert_config(config)
    environment = cfg["environment"]
    table_name = cfg["table_name"]
    db_schema = cfg["db_schema"]

    if metrics is None:
        metrics = default_metrics()

    start = datetime.now()

    if df is None or df.empty:
        return _mark_empty_df_metrics(metrics, environment, table_name, start)

    metrics["total"] = len(df)
    metrics["run_id"] = f"{table_name}_{start.strftime('%d-%m-%Y_%H%M%S')}"

    try:
        engine = create_engine(mysql_uri)
        log(f"[{environment.upper()}] conectando ao MySQL...", level="info")

        with engine.begin() as conn:
            if cfg["custom_insert_query"]:
                df_req, query = _prepare_custom_insert(
                    df=df,
                    custom_insert_query=cfg["custom_insert_query"],
                    db_schema=db_schema,
                    metrics=metrics,
                    start=start,
                )
                if df_req is None or query is None:
                    return metrics
                _run_custom_insert(conn, df_req, query, cfg["batch_size"], metrics)
            else:
                _run_to_sql_insert(conn, df, cfg, metrics)

    except SQLAlchemyError as exc:
        msg = f"Erro na conexão MySQL: {exc}"
        log(msg, level="error")
        metrics["failed"] = metrics["total"]
        metrics["errors"] = [msg]

    metrics["execution_time"] = (datetime.now() - start).total_seconds()
    log(
        f"[{environment.upper()}] inserção finalizada: "
        f"inseridos={metrics['inserted']}, falhas={metrics['failed']}, "
        f"tempo={metrics['execution_time']:.2f}s",
        level="info",
    )
    return metrics


def _status_emoji(total: int, inserted: int, failed: int, notes: List[str]) -> str:
    if failed > 0:
        return "❌"
    if inserted == 0 and total == 0 and notes:
        return "⚠️"
    return "✅"


def _format_duration(duration: float) -> str:
    if duration < 60:
        return f"{duration:.1f} segundos"
    m, s = divmod(int(duration), 60)
    return f"{m}m {s}s ({duration:.1f} segundos)"


def _build_report_lines(project_clean: str, metrics: Dict[str, Any]) -> List[str]:
    total = int(metrics.get("total", 0))
    inserted = int(metrics.get("inserted", 0))
    failed = int(metrics.get("failed", 0))
    duration = float(metrics.get("execution_time", 0.0))
    errors = metrics.get("errors", []) or []
    notes = metrics.get("notes", []) or []
    run_id = metrics.get("run_id", "") or ""

    emoji = _status_emoji(total, inserted, failed, notes)

    lines = [
        f"## {emoji} **{project_clean}** - {datetime.now().strftime('%d/%m/%Y - %H:%M:%S')}",
        "",
        "**Resumo:**",
        f"- Total de registros: `{total}`",
        f"- Inseridos com sucesso: `{inserted}`",
        f"- Falhas: `{failed}`",
        f"- Duração: `{_format_duration(duration)}`",
        f"- Run ID: `{run_id}`",
    ]

    if errors:
        lines += ["", "**Erros:**"]
        for e in errors[:5]:
            lines.append(f"```{summarize_error(e)}```")

    if notes:
        lines += ["", "**Observações:**"]
        lines += [f"- {n}" for n in notes]

    return lines


@task
def generate_report(metrics: dict, context: Optional[Dict[str, Any]] = None) -> str:
    """
    Gera relatório de execução para logs e notificações.
    """
    ctx = {**DEFAULT_REPORT_CONTEXT, **(context or {})}

    project_clean = clean_str(ctx.get("project", ""))
    lines = _build_report_lines(project_clean, metrics)
    report = "\n".join(lines)

    log(report, level="info")

    notify = bool(ctx.get("notify"))
    environment = str(ctx.get("environment", "dev"))
    failed = int(metrics.get("failed", 0))
    errors = metrics.get("errors", []) or []
    notes = metrics.get("notes", []) or []

    if notify and (environment == "dev" or failed > 0 or errors or notes):
        send_message(
            title=str(ctx.get("pipeline_name", DEFAULT_REPORT_CONTEXT["pipeline_name"])),
            message=report,
            monitor_slug=str(ctx.get("monitor_slug", DEFAULT_REPORT_CONTEXT["monitor_slug"])),
        )

    return report

@task
def apply_df_filter(df, filter_name: str | None, notes: list | None = None):
    """Aplica um filtro opcional no DF, controlado por config."""
    if df is None or getattr(df, "empty", True) or not filter_name:
        return df

    fn = DF_FILTERS.get(filter_name)
    if not fn:
        msg = f"df_filter_name='{filter_name}' não encontrado; seguindo sem filtro."
        log(msg, level="warning")
        if notes is not None:
            notes.append(msg)
        return df

    return fn(df, notes=notes)
