# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Dump
"""
from datetime import timedelta

import pandas as pd
from pipelines.utils.credential_injector import authenticated_task as task
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.smsrio_mysql_general.constants import (
    constants as smsrio_constants,
)


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def download_from_db(
    db_url: str,
    db_schema: str,
    db_table: str,
    file_folder: str,
    file_name: str,
) -> None:
    """
    Downloads data from a database table and saves it as a CSV file.

    Args:
        db_url (str): The URL of the database.
        db_schema (str): The schema of the database.
        db_table (str): The name of the table to download data from.
        file_folder (str): The folder where the CSV file will be saved.
        file_name (str): The name of the CSV file.

    Returns:
        str: The file path of the downloaded CSV file.
    """

    connection_string = f"{db_url}/{db_schema}"

    query = f"SELECT * FROM {db_table}"

    table = pd.read_sql(query, connection_string)

    destination_file_path = f"{file_folder}/{file_name}.csv"

    table.to_csv(destination_file_path, index=False, sep=";", encoding="utf-8")

    log(f"File {destination_file_path} downloaded from DB.")

    return destination_file_path


@task
def build_gcp_table(db_table: str) -> str:
    """Generate the GCP table name from the database table name."""
    return smsrio_constants.TABLE_ID.value[db_table]
