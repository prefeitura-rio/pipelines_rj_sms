# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Dump
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
    datetime_column: str,
    id_column: str,
    batch_size: int = 50000,
    date_filter: datetime = None,
) -> list[str]:

    sql_filter = ""
    if date_filter:
        sql_filter = f"WHERE {datetime_column} >= '{date_filter.strftime('%Y-%m-%d')}'"

    total_rows = pd.read_sql(
        f"SELECT COUNT(*) as quant FROM {db_schema}.{db_table} {sql_filter}", db_url
    ).iloc[0]["quant"]
    log(f"Total rows to download: {total_rows}")

    if total_rows <= batch_size:
        return [f"SELECT * FROM {db_schema}.{db_table} {sql_filter}"]

    num_batches = ceil(total_rows / batch_size)
    log(f"Number of batches to download: {num_batches}")

    queries = []
    for i in range(num_batches):
        query = f"""
            SELECT * FROM {db_schema}.{db_table} {sql_filter}
            ORDER BY {id_column} ASC
            LIMIT {batch_size} OFFSET {i * batch_size}
        """
        log(f"Query {i+1}: {query}")
        queries.append(query)

    return queries


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def download_from_db(
    db_url: str,
    query: str,
) -> pd.DataFrame:

    table = pd.read_sql(query, db_url)
    log(f"Downloaded {len(table)} rows from Table")

    table["datalake_loaded_at"] = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    table.columns = remove_columns_accents(table)
    return table


@task
def build_bq_table_name(db_table: str, schema: str) -> str:
    return f"{schema}__{db_table}"
