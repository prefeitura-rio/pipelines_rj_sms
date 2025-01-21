# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Dump
"""
from datetime import timedelta

import pandas as pd
import prefect
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.historico_clinico_integrado.constants import (
    constants as hci_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def download_from_db(
    db_url: str,
    db_table: str,
    target_date: str,
    file_folder: str,
    file_name: str,
    historical_mode: bool,
    reference_datetime_column: str,
) -> None:
    """
    Downloads data from a database table and saves it as a CSV file.

    Args:
        db_url (str): The URL of the database.
        db_schema (str): The schema of the database.
        db_table (str): The name of the table to download data from.
        target_date (str): Target date of run
        file_folder (str): The folder where the CSV file will be saved.
        file_name (str): The name of the CSV file.
        historical_mode (bool): Boolean to historical extraction
        reference_datetime_column (str): Name of the column that contains the reference datetime.

    Returns:
        str: The file path of the downloaded CSV file.
    """

    query = f"SELECT * FROM {db_table}"

    if not historical_mode:
        if not target_date or target_date == "":
            target_date = prefect.context.get("scheduled_start_time").date()
        window_date = pd.to_datetime(target_date).date() - timedelta(days=7)
        query += f" WHERE {reference_datetime_column} BETWEEN '{window_date}' AND '{target_date}'"

    log(query)

    table = pd.read_sql(query, db_url)
    log(f"{len(table)} rows downloaded")

    destination_file_path = f"{file_folder}/{file_name}_{target_date}.csv"
    table.to_csv(destination_file_path, index=False, sep=";", encoding="utf-8")
    log(f"File {destination_file_path} downloaded from DB.")

    return destination_file_path


@task
def build_gcp_table(db_table: str) -> str:
    """Generate the GCP table name from the database table name."""

    if db_table not in hci_constants.TABLE_ID.value:
        return db_table

    return hci_constants.TABLE_ID.value[db_table]
