# -*- coding: utf-8 -*-
import datetime
import os

import pandas as pd
import psycopg2
from google.cloud import bigquery
from sqlalchemy.exc import ProgrammingError

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def list_tables_to_import():
    return [
        "paciente",
        "alergia",
        "atendimento",
        "boletim",
        "cirurgia",
        "classificacao_risco",
        "diagnostico",
        "exame",
        "profissional",
    ]


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=120))
def get_last_timestamp_from_tables(
    project_name: str, dataset_name: str, table_names: list[str], column_name: str
) -> pd.DataFrame:

    client = bigquery.Client()

    subqueries = [
        f"""
        SELECT MAX({column_name}) as max_value, "{table_name}" as table_name
        FROM `{project_name}.{dataset_name}_staging.{table_name}`
        """
        for table_name in table_names
    ]

    query = " UNION ALL ".join(subqueries)
    results = client.query(query).result().to_dataframe()

    # Order values as in table_names list
    max_values = []
    for table_name in table_names:
        max_values.append(results[results["table_name"] == table_name].max_value.iloc[0])

    return max_values


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=120))
def import_vitai_table_to_csv(
    db_url: str,
    table_name: str,
    interval_start: pd.Timestamp,
    interval_end: pd.Timestamp = None,
) -> str:
    if interval_end is None:
        interval_end = pd.Timestamp.now(tz="America/Sao_Paulo")

    
    interval_start = interval_start.strftime("%Y-%m-%d %H:%M:%S")
    interval_end = interval_end.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        select *
        from basecentral.{table_name}
        where datahora >= '{interval_start}' and < '{interval_end}'
        """
    log("Executing query:")
    log(query)

    try:
        df = pd.read_sql(
            query,
            db_url,
            dtype=str,
        )
    except ProgrammingError as e:
        if isinstance(e.orig, psycopg2.errors.InsufficientPrivilege):
            log(
                f"Insufficient privilege to table basecentral.`{table_name}`. Ignoring table.",  # noqa
                level="error",
            )
            return ""
        raise

    if "id" in df.columns:
        log("Detected `id` column in dataframe. Renaming to `gid`", level="warning")
        df.rename(columns={"id": "gid"}, inplace=True)

    df["datalake__imported_at"] = pd.Timestamp.now(tz="America/Sao_Paulo")
    log(
        f"Added `imported_at` column to dataframe with current timestamp: {df['datalake__imported_at'].iloc[0]}"  # noqa
    )

    if not os.path.isdir("./tabledata"):
        log("Creating tabledata directory")
        os.mkdir("./tabledata")

    file_path = f"""./tabledata/{table_name}-{interval_start}-{interval_end}.csv"""  # noqa
    log(f"Saving table data to {file_path}")
    df.to_csv(file_path, index=False, header=True, sep=";")

    return file_path


@task(nout=2)
def get_upload_params_in_config(config: dict):
    return config["file_path"], config["table_name"]
