# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Dump
"""
from datetime import timedelta

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.historico_clinico_integrado.constants import (
    constants as hci_constants,
)
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
    """
    Downloads data from a database table and saves it as a CSV file.

    Args:
        db_url (str): The URL of the database.
        db_schema (str): The schema of the database.
        db_table (str): The name of the table to download data from.
        start_target_date (str): Start target date of run
        end_target_date (str): End target date of run
        historical_mode (bool): Boolean to historical extraction
        reference_datetime_column (str): Name of the column that contains the reference datetime.

    Returns:
        str: The file path of the downloaded CSV file.
    """

    query = f"SELECT * FROM {db_schema}.{db_table}"

    if not historical_mode:
        query += f" WHERE {reference_datetime_column} BETWEEN '{start_target_date}' AND '{end_target_date}'" # noqa

    log(query)

    table = pd.read_sql(query, db_url)
    table["datalake_loaded_at"] = pd.Timestamp.now(tz="America/Sao_Paulo")
    log(f"{len(table)} rows downloaded")

    return table


@task
def build_gcp_table(db_table: str, schema: str) -> str:
    """Generate the GCP table name from the database table name."""

    return f"{schema}__{db_table}"
