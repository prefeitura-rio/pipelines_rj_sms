# -*- coding: utf-8 -*-
# pylint: disable=import-error

"""
Funções utilitárias para pipeline de migração BigQuery → MySQL da SUBPAV.
Inclui helpers de ambiente, validação de query, batch execution, métricas e validação de colunas.
"""
import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from pipelines.utils.logger import log


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


def _execute_batches(
    conn,
    query: str,
    records: List[Dict[str, Any]],
    batch_size: int,
    max_retries: int = 1,
) -> Tuple[int, int, List[str]]:
    """
    Executa inserts/updates em lotes usando SQLAlchemy.
    Retorna: (success, failure, errors_resumidos).
    """
    total = len(records)
    success = 0
    failure = 0
    errors: List[str] = []

    for i in range(0, total, batch_size):
        batch = records[i : i + batch_size]
        msg = f"Processing batch {i}-{i + len(batch) - 1} " f"({len(batch)} records)..."
        log(msg, level="info")

        attempt = 0
        while attempt < max_retries:
            try:
                conn.execute(text(query), batch)
                success += len(batch)
                break
            except SQLAlchemyError as exc:
                attempt += 1
                if attempt >= max_retries:
                    failure += len(batch)
                    err_msg = (
                        f"❌ Batch {i}-{i + len(batch) - 1} failed: " f"{str(exc).splitlines()[0]}"
                    )
                    log(err_msg, level="error")
                    if len(errors) < 5:
                        errors.append(err_msg)
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
        c = c.strip()
        if c:
            valid.append(c)
    return valid


def ensure_dataframe_columns(
    df: pd.DataFrame, required: List[str], fill_value=None, notes: list = None
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
