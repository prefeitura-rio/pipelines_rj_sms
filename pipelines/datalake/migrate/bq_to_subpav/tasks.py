# -*- coding: utf-8 -*-
# pylint: disable=import-error

"""
Tasks para migração de dados do BigQuery para o MySQL da SUBPAV.
"""
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
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
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import get_secret_key

@task
def resolve_notify(project: str, notify_param) -> bool:
    """
    Valida necessidade de notificação do relatorio
    """
    return notify_param if notify_param is not None else should_notify(project)


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def query_bq_table(
    dataset_id: str,
    table_id: str,
    environment: str,
    bq_columns: Optional[List[str]] = None,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Executa a consulta no BigQuery
    """
    project_id = "rj-sms"
    full_ref = f"{project_id}.{dataset_id}.{table_id}"
    bq_columns = validate_bq_columns(bq_columns)

    def _safe_column(col: str) -> str:
        return re.sub(r"[^\w\.\s]", "", col)

    columns_clause = ", ".join([_safe_column(c) for c in bq_columns]) if bq_columns else "*"
    limit_clause = f"LIMIT {limit}" if limit else ""
    query = f"SELECT {columns_clause} FROM `{full_ref}` {limit_clause}"

    try:
        client = bigquery.Client()
        df = client.query(query).to_dataframe()
        log(f"[{environment.upper()}] {full_ref} → {len(df)} registros", level="info")
        return {"df": df, "metrics": None}
    except Exception as e:
        msg = (
            f"[{environment.upper()}] Erro ao consultar BigQuery ({full_ref}): "
            f"{type(e).__name__}: {e}"
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


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def insert_df_into_mysql(
    df: pd.DataFrame,
    mysql_uri: str,
    db_schema: str,
    table_name: str,
    if_exists: str = "append",
    custom_insert_query: Optional[str] = None,
    environment: str = "dev",
    batch_size: int = 1000,
    metrics: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Responsável pela inserção de dados no Mysql
    """
    if metrics is None:
        metrics = default_metrics()
    start = datetime.now()

    if df is None or df.empty:
        msg = f"[{environment.upper()}] sem registros para inserir em {table_name}"
        log(msg, level="info")
        metrics["notes"].append(msg)
        metrics["execution_time"] = (datetime.now() - start).total_seconds()
        return metrics

    metrics["total"] = len(df)
    metrics["run_id"] = f"{table_name}_{start.strftime('%d-%m-%Y_%H%M%S')}"

    try:
        engine = create_engine(mysql_uri)
        log(f"[{environment.upper()}] conectando ao MySQL...", level="info")

        with engine.begin() as conn:
            # Validação quando existir query customizada
            if custom_insert_query:
                custom_insert_query = inject_db_schema_in_query(custom_insert_query, db_schema)
                if not format_query(custom_insert_query):
                    msg = "❌ Query customizada fora do padrão permitido!"
                    log(msg, level="error")
                    metrics["errors"].append("Query customizada fora do padrão permitido.")
                    metrics["failed"] = metrics["total"]
                    metrics["execution_time"] = (datetime.now() - start).total_seconds()
                    return metrics

                required_columns = extract_query_params(custom_insert_query)
                if not required_columns:
                    msg = (
                        "Não foi possível identificar parâmetros obrigatórios na query customizada."
                    )
                    log(msg, level="error")
                    metrics["errors"].append(msg)
                    metrics["failed"] = len(df)
                    metrics["execution_time"] = (datetime.now() - start).total_seconds()
                    return metrics

                df = ensure_dataframe_columns(
                    df, required_columns, fill_value=None, notes=metrics["notes"]
                )

                campos_none = [col for col in required_columns if df[col].isnull().all()]
                if campos_none:
                    warn_msg = (
                        f"Atenção: as colunas {', '.join(campos_none)} "
                        "foram preenchidas somente com None (cheque o schema de destino)."
                    )
                    log(warn_msg, level="warning")
                    metrics["notes"].append(warn_msg)

                recs = df.where(pd.notnull(df), None).to_dict(orient="records")

                ok, err = _execute_batches(
                    conn, custom_insert_query, recs, batch_size, metrics["errors"]
                )
                metrics["inserted"] = ok
                metrics["failed"] = err

            else:
                try:
                    df.to_sql(
                        name=table_name,
                        con=conn,
                        schema=db_schema,
                        if_exists=if_exists,
                        index=False,
                        chunksize=batch_size,
                    )
                    metrics["inserted"] = metrics["total"]
                except Exception as e:
                    msg = f"❌Falha na inserção: {e}"
                    log(msg, level="error")
                    metrics["failed"] = metrics["total"]
                    metrics["errors"].append(msg)
    except SQLAlchemyError as e:
        msg = f"Erro na conexão MySQL: {e}"
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


@task
def generate_report(
    metrics: dict,
    pipeline_name: str = "Migração BQ→MySQL SUBPAV",
    monitor_slug: str = "subpav",
    project: str = "",
    notify: bool = False,
    environment: str = "dev",
) -> str:
    """
    Gera relatório de execução para logs e notificações.
    """

    total = metrics.get("total", 0)
    inserted = metrics.get("inserted", 0)
    failed = metrics.get("failed", 0)
    duration = metrics.get("execution_time", 0.0)
    errors = metrics.get("errors", [])
    notes = metrics.get("notes", [])
    run_id = metrics.get("run_id", "")

    project_clean = clean_str(project)
    if failed > 0:
        emoji_status = "❌"
    elif inserted == 0 and total == 0 and notes:
        emoji_status = "⚠️"
    else:
        emoji_status = "✅"

    # Cabeçalho
    lines = [
        f"## {emoji_status} **{project_clean}** - {datetime.now().strftime('%d/%m/%Y - %H:%M:%S')}",
        "",
        "**Resumo:**",
        f"- Total de registros: `{total}`",
        f"- Inseridos com sucesso: `{inserted}`",
        f"- Falhas: `{failed}`",
    ]

    if duration < 60:
        lines.append(f"- Duração: `{duration:.1f} segundos`")
    else:
        m, s = divmod(int(duration), 60)
        lines.append(f"- Duração: `{m}m {s}s ({duration:.1f} segundos)`")

    lines.append(f"- Run ID: `{run_id}`")

    if errors:
        lines.append("")
        lines.append("**Erros:**")
        for e in errors[:5]:
            lines.append(f"```{summarize_error(e)}```")

    # Observações
    if notes:
        lines.append("")
        lines.append("**Observações:**")
        for note in notes:
            lines.append(f"- {note}")

    report = "\n".join(lines)
    log(report, level="info")

    if notify and (environment == "dev" or failed > 0 or errors or notes):
        send_message(title=f"{pipeline_name}", message=report, monitor_slug=monitor_slug)
    return report
