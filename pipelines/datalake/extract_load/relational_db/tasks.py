# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Dump
"""
from datetime import timedelta

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def download_from_db(
    db_url: str,
    db_schema: str,
    db_table: str,
    start_target_date: str,
    end_target_date: str,
    historical_mode: bool,
    reference_datetime_column: str,
) -> None:

    query = f"SELECT * FROM {db_schema}.{db_table}"

    if not historical_mode:
        query += f" WHERE {reference_datetime_column} BETWEEN '{start_target_date}' AND '{end_target_date}'"  # noqa

    log(query)

    table = pd.read_sql(query, db_url)
    table["loaded_at"] = pd.Timestamp.now(tz="America/Sao_Paulo")
    log(f"{len(table)} rows downloaded")

    return table


@task
def build_gcp_table(db_table: str, schema: str) -> str:
    """Generate the GCP table name from the database table name."""

    return f"{schema}__{db_table}"
