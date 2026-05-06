# -*- coding: utf-8 -*-
# pylint: disable=import-error

"""
Funções utilitárias para pipeline de migração BigQuery → MySQL da SUBPAV.
Inclui helpers de ambiente, validação de query, batch execution, métricas e validação de colunas.
"""
import re
import time
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pipelines.utils.logger import log

RETRYABLE_MYSQL_PATTERNS = (
    "lock wait timeout exceeded",
    "deadlock found",
    "try restarting transaction",
    "(1205,",
    "(1213,",
)


def should_notify(project: str = None) -> bool:
    """
    Define se o fluxo deve notificar com base na presença do parâmetro 'project'.
    """
    return bool(project)


def format_query(query: str) -> bool:
    """
    Valida se a query customizada é segura para execução em lote.
    Por padrão, aceita INSERT, UPDATE e REPLACE INTO.
    """
    if not query:
        return False

    query = query.strip().rstrip(";")

    first_word = re.match(r"^\s*(INSERT|UPDATE|REPLACE)\b", query, re.IGNORECASE)
    if not first_word:
        return False

    forbidden_keywords = ["DROP", "DELETE", "ALTER", "TRUNCATE", "EXEC", "MERGE"]
    if any(re.search(rf"\b{kw}\b", query, re.IGNORECASE) for kw in forbidden_keywords):
        return False

    if ";" in query:
        return False

    if re.search(r"(--|/\*|\*/)", query):
        return False

    return True


def _is_retryable_mysql_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(pattern in msg for pattern in RETRYABLE_MYSQL_PATTERNS)


def _execute_batches(
    engine,
    query: str,
    records: List[Dict[str, Any]],
    execution_config: Dict[str, int],
) -> Tuple[int, int, List[str]]:
    """
    Executa inserts/updates em lotes usando SQLAlchemy.
    Cada lote é confirmado separadamente para reduzir janela de lock.
    Retorna: (success, failure, errors_resumidos).
    """
    total = len(records)
    success = 0
    failure = 0
    errors: List[str] = []
    batch_size = execution_config["batch_size"]
    max_retries = execution_config["max_retries"]
    retry_backoff_seconds = execution_config["retry_backoff_seconds"]

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        log(f"Processing batch {i}-{i + len(batch) - 1} ({len(batch)} records)...", level="info")

        attempt = 0
        while attempt < max_retries:
            attempt += 1
            try:
                with engine.begin() as conn:
                    conn.execute(text(query), batch)
                success += len(batch)
                break
            except SQLAlchemyError as exc:
                retryable = _is_retryable_mysql_error(exc)

                if retryable and attempt < max_retries:
                    wait_seconds = retry_backoff_seconds * attempt
                    log(
                        (
                            f"Batch {i}-{i + len(batch) - 1} com erro transitório "
                            f"(tentativa {attempt}/{max_retries}): {str(exc).splitlines()[0]}. "
                            f"Novo retry em {wait_seconds}s."
                        ),
                        level="warning",
                    )
                    time.sleep(wait_seconds)
                    continue

                failure += len(batch)
                err_msg = f"❌ Batch {i}-{i + len(batch) - 1} failed: {str(exc).splitlines()[0]}"
                log(err_msg, level="error")
                if len(errors) < 5:
                    errors.append(err_msg)
                break

    return success, failure, errors


def default_metrics() -> Dict[str, Any]:
    """
    Retorna o template de métricas vazio.
    """
    return {
        "project": "",
        "total": 0,
        "inserted": 0,
        "failed": 0,
        "errors": [],
        "execution_time": 0.0,
        "notes": [],
        "run_id": "",
    }


def clean_str(s: str) -> str:
    """
    Normaliza string para evitar problemas de acentuação e encoding.
    """
    if not s:
        return ""
    return unicodedata.normalize("NFKC", str(s))


def summarize_error(msg: str, max_length: int = 350) -> str:
    """
    Reduz mensagens de erro para facilitar visualização no relatório/Discord.
    Mostra o início do erro e um resumo da query, se for o caso.
    """
    lines = msg.splitlines()
    summary = lines[0]
    # Se tem comando SQL, mostrar só o início da query
    if "SQL:" in msg:
        sql_start = msg.find("SQL:")
        summary += "\n" + msg[sql_start : sql_start + 180] + " ... [truncado]"
    elif len(msg) > max_length:
        summary = msg[:max_length] + " ... [truncado]"
    return summary


def validate_bq_columns(bq_columns: Optional[List[str]]) -> List[str]:
    """
    Remove colunas vazias ou inválidas da lista para evitar erros de query.
    """
    if not bq_columns:
        return []

    valid = []
    for c in bq_columns:
        if c is None:
            continue
        c = str(c).strip()
        if c:
            valid.append(c)
    return valid


def ensure_dataframe_columns(
    df: pd.DataFrame,
    required: List[str],
    fill_value=None,
    notes: Optional[List[str]] = None,
):
    """
    Garante que todas as colunas obrigatórias estejam presentes no DataFrame.
    """
    df = df.copy()
    missing = [col for col in required if col not in df.columns]
    if missing:
        msg = (
            f"Colunas ausentes no DataFrame: {', '.join(missing)}. Preenchidas com {fill_value!r}."
        )
        if notes is not None:
            notes.append(msg)
        for col in missing:
            df[col] = fill_value
    return df[required]


def extract_query_params(query: str) -> List[str]:
    """
    Extrai todos os parâmetros nomeados de uma query customizada.
    Suporta formatos:
    - %(campo)s      (padrão Python/MySQL)
    - :campo         (padrão SQLAlchemy)
    """
    if not query:
        return []
    # Pega %(campo)s
    params_percent = re.findall(r"%\((\w+)\)s", query)
    # Pega :campo
    params_colon = re.findall(r":(\w+)", query)
    # Remove duplicados preservando ordem
    seen = set()
    params = []
    for p in params_percent + params_colon:
        if p not in seen:
            seen.add(p)
            params.append(p)
    return params


def inject_db_schema_in_query(query: str, db_schema: str) -> str:
    """
    Adiciona o schema/database na query customizada caso não esteja presente.
    Só altera o nome da tabela logo após o comando (INSERT INTO, UPDATE, REPLACE INTO).
    """
    if not db_schema or not query:
        return query

    pattern = re.compile(
        r"\b(INSERT(?:\s+IGNORE)?\s+INTO|UPDATE|REPLACE\s+INTO)\s+([`]?)([\w\.]+)([`]?)",
        re.IGNORECASE,
    )

    def replacer(match):
        comando = match.group(1)
        aspas_esquerda = match.group(2)
        tabela = match.group(3)
        aspas_direita = match.group(4)
        if "." in tabela:
            return match.group(0)
        return f"{comando} {aspas_esquerda}{db_schema}.{tabela}{aspas_direita}"

    return pattern.sub(replacer, query, count=1)


def _col(df: pd.DataFrame, name: str, default=None) -> pd.Series:
    """Retorna uma Series do tamanho do DF mesmo se a coluna não existir."""
    if name in df.columns:
        return df[name]
    return pd.Series([default] * len(df), index=df.index)


def _is_non_empty_str(s: pd.Series) -> pd.Series:
    """True para strings não vazias (tratando NaN)."""
    return s.fillna("").astype(str).str.strip().ne("")


def filter_exames_update_sintomatico(
    df: pd.DataFrame,
    notes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Mantém somente exames que já conseguiram amarrar com um id_sintomatico
    e ordena os registros para atualização consistente no MySQL.
    """
    del notes
    if df is None or df.empty:
        return df

    df = df.copy()

    # garante tipos mínimos
    if "id_sintomatico" in df.columns:
        df["id_sintomatico"] = pd.to_numeric(df["id_sintomatico"], errors="coerce")

    if "id_tipo_exame" in df.columns:
        df["id_tipo_exame"] = pd.to_numeric(df["id_tipo_exame"], errors="coerce")

    if "id_resultado" in df.columns:
        df["id_resultado"] = pd.to_numeric(df["id_resultado"], errors="coerce")

    if "dt_resultado" in df.columns:
        df["dt_resultado"] = pd.to_datetime(df["dt_resultado"], errors="coerce").dt.date

    # só atualiza sintomático quando já houver vínculo explícito
    df = df[
        df["id_sintomatico"].notna()
        & df["id_tipo_exame"].isin([1, 2, 3])
        & df["id_resultado"].notna()
        & df["dt_resultado"].notna()
    ].copy()

    colunas_ordem = ["id_sintomatico", "id_tipo_exame", "dt_resultado"]
    if "codigo_amostra" in df.columns:
        colunas_ordem.append("codigo_amostra")

    colunas_ordem = [c for c in colunas_ordem if c in df.columns]
    df = df.sort_values(colunas_ordem, ascending=True)

    chaves = ["id_sintomatico", "id_tipo_exame", "dt_resultado"]
    if "codigo_amostra" in df.columns:
        chaves.append("codigo_amostra")

    chaves_existentes = [c for c in chaves if c in df.columns]
    df = df.drop_duplicates(subset=chaves_existentes, keep="last").reset_index(drop=True)

    return df


def filter_update_sintomatico_gal(
    df: pd.DataFrame,
    notes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Mantém somente exames elegíveis para atualização de sintomáticos criados via GAL.
    """
    del notes

    if df is None or df.empty:
        return df

    df = df.copy()

    df = df[
        (df["id_tipo_exame"].isin([1, 2, 3]))
        & (df["id_resultado"].notna())
        & (df["dt_resultado"].notna())
        & (df["paciente_cpf"].notna())
        & (df["diagnostico"].isin([1]))
    ].copy()

    colunas_ordem = ["paciente_cpf", "id_tipo_exame", "dt_resultado"]
    if "codigo_amostra" in df.columns:
        colunas_ordem.append("codigo_amostra")

    colunas_ordem = [c for c in colunas_ordem if c in df.columns]
    df = df.sort_values(colunas_ordem, ascending=True)

    chaves = ["paciente_cpf", "id_tipo_exame", "dt_resultado"]
    if "codigo_amostra" in df.columns:
        chaves.append("codigo_amostra")

    chaves_existentes = [c for c in chaves if c in df.columns]
    df = df.drop_duplicates(subset=chaves_existentes, keep="last").reset_index(drop=True)

    return df
