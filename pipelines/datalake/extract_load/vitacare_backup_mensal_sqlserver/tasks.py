# -*- coding: utf-8 -*-
"""
Tasks for Vitacare Historic SQL Server 
"""
from datetime import datetime, timedelta
from math import ceil
import pandas as pd
import pytz

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def create_extraction_batches(
    db_url: str,
    db_schema: str,
    db_table: str,
    id_column: str,
    batch_size: int = 50000,
    date_filter: str = None,
) -> list[str]:
    """
    Gera queries SQL para extrair a tabela em lotes, usando OFFSET/FETCH (SQL Server).
    """
    sql_filter = f"WHERE {id_column} IS NOT NULL"
    if date_filter:
        sql_filter += f" AND {id_column} >= '{date_filter}'"

    total_query = f"SELECT COUNT(*) AS quant FROM {db_schema}.{db_table} {sql_filter}"
    total_rows = pd.read_sql(total_query, db_url).iloc[0]["quant"]
    log(f"Total rows to download from {db_table}: {total_rows}")

    if total_rows == 0:
        log(f"Tabela {db_table} vazia. Nenhuma extração necessária.")
        return []

    if total_rows <= batch_size:
        return [f"SELECT * FROM {db_schema}.{db_table} {sql_filter} ORDER BY {id_column} ASC"]

    num_batches = ceil(total_rows / batch_size)
    log(f"Number of batches to download from {db_table}: {num_batches}")

    queries = []
    for i in range(num_batches):
        query = f"""
            SELECT * FROM {db_schema}.{db_table}
            {sql_filter}
            ORDER BY {id_column} ASC
            OFFSET {i * batch_size} ROWS
            FETCH NEXT {batch_size} ROWS ONLY
        """
        log(f"Query {i+1} for {db_table}: {query}")
        queries.append(query)

    return queries


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def download_from_db(
    db_url: str,
    query: str,
    cnes_code: str,
) -> pd.DataFrame:
    """
    Executa a query, baixa os dados e adiciona colunas extras.
    """
    table = pd.read_sql(query, db_url)
    log(f"Downloaded {len(table)} rows with query: {query[:60]}...")

    now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    table["extracted_at"] = now
    table["id_cnes"] = cnes_code

    table.columns = remove_columns_accents(table)
    return table


@task
def build_bq_table_name(table_name: str) -> str:
    """
    Retorna o nome da tabela padronizado para o BigQuery.
    """
    return f"{table_name}_historic" 