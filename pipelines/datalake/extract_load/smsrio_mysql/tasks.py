# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Dump
"""
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
from dateutil.tz import gettz
from pipelines.datalake.extract_load.smsrio_mysql.constants import (
    constants as smsrio_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def create_extraction_batches(
    db_url: str,
    db_schema: str,
    db_table: str,
    batch_size: int = 1000000,
    date_filter: Optional[datetime] = None,
) -> list[str]:

    sql_filter = ""
    if date_filter:
        sql_filter = f"WHERE timestamp >= '{date_filter.strftime('%Y-%m-%d')}'"

    total_rows = pd.read_sql(
        f"SELECT COUNT(*) as quant FROM {db_schema}.{db_table} {sql_filter}", db_url
    )["quant"].values[0]
    log(f"Total rows to download: {total_rows}")

    if total_rows <= batch_size:
        return [f"SELECT * FROM {db_schema}.{db_table} {sql_filter}"]

    num_batches = total_rows // batch_size
    log(f"Number of batches to download: {num_batches}")

    queries = []
    for i in range(num_batches):
        query = f"""
            SELECT * FROM {db_schema}.{db_table} {sql_filter}
            ORDER BY id ASC
            LIMIT {batch_size} OFFSET {i * batch_size}
        """
        log(f"Query {i+1}: {query}")
        queries.append(query)

    return queries


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def download_from_db(
    db_url: str,
    query: str,
) -> pd.DataFrame:

    table = pd.read_sql(query, db_url)
    log(f"Downloaded {len(table)} rows from Table")

    table["datalake_loaded_at"] = datetime.now(tz=gettz("America/Sao_Paulo"))
    return table


@task
def build_gcp_table(db_table: str) -> str:
    if db_table in smsrio_constants.TABLE_ID.value:
        return smsrio_constants.TABLE_ID.value[db_table]
    else:
        log(f"Table {db_table} not found in constants, returning original table name")
        return db_table
