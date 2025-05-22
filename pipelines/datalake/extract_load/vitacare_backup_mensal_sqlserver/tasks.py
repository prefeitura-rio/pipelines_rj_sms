# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""
from datetime import datetime, timedelta

import pandas as pd
import pytz
from sqlalchemy import create_engine

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def extract_and_transform_table(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_name: str,
    db_schema: str,
    db_table: str,
    cnes_code: str,
) -> pd.DataFrame:
    full_table_name = f"{db_schema}.{db_table}"
    log(f"Attempting to download data from {full_table_name} for CNES: {cnes_code}")

    try:
        connection_string = (
            f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?"
            "driver=ODBC Driver 17 for SQL Server"
        )
        engine = create_engine(connection_string)

        df = pd.read_sql(f"SELECT * FROM {full_table_name}", engine)
        log(f"Successfully downloaded {len(df)} rows from {full_table_name}.")

        now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        df["extracted_at"] = now
        df["id_cnes"] = cnes_code

        df.columns = remove_columns_accents(df)
        log(f"Transformed DataFrame for {full_table_name} from CNES {cnes_code}.")
        return df

    except Exception as e:
        log(f"Error downloading or transforming data from {full_table_name} (CNES: {cnes_code}): {e}", level="error")
        raise


@task
def build_bq_table_name(table_name: str) -> str:
    bq_table_name = f"{table_name.lower()}_historic"
    log(f"Built BigQuery table name: {bq_table_name} for source table: {table_name}")
    return bq_table_name