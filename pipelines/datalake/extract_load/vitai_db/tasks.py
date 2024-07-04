# -*- coding: utf-8 -*-
import datetime
import os
import uuid

import google
import pandas as pd
import prefect
from google.cloud import bigquery
from prefect.backend import FlowRunView

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def list_tables_to_import():
    return [
        "paciente",
        "boletim",
        "alergia",
        "atendimento",
        "cirurgia",
        "classificacao_risco",
        "diagnostico",
        "exame",
        "profissional",
    ]


@task()
def get_bigquery_project_from_environment(environment: str) -> str:
    if environment in ["prod", "local-prod"]:
        return "rj-sms"
    elif environment in ["dev", "staging", "local-staging"]:
        return "rj-sms-dev"
    else:
        raise ValueError(f"Invalid environment: {environment}")


@task()
def get_interval_start_list(interval_start: str, table_names: list[str]) -> list:
    value = pd.to_datetime(interval_start, format="%Y-%m-%d %H:%M:%S")
    return [value for _ in table_names]


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=120))
def get_last_timestamp_from_tables(
    project_name: str, dataset_name: str, table_names: list[str], column_name: str
) -> list:

    client = bigquery.Client()

    default_max_value = pd.Timestamp.now(tz="America/Sao_Paulo") - pd.Timedelta(minutes=30)

    max_values = []
    for table_name in table_names:
        query = f"""
        SELECT MAX({column_name}) as max_value, "{table_name}" as table_name
        FROM `{project_name}.{dataset_name}_staging.{table_name}`
        """

        try:
            result = client.query(query).result().to_dataframe().max_value.iloc[0]
            if result is None:
                log(f"Table {table_name} is empty. Ignoring table.", level="warning")
                max_value = default_max_value
            else:
                max_value = pd.to_datetime(result, format="%Y-%m-%d %H:%M:%S.%f")
        except google.api_core.exceptions.NotFound:
            log(f"Table {table_name} not found in BigQuery. Ignoring table.", level="warning")
            max_value = default_max_value

        max_values.append(max_value)

    return max_values


@task(max_retries=3, retry_delay=datetime.timedelta(seconds=120))
def import_vitai_table_to_csv(
    db_url: str,
    table_name: str,
    output_file_folder: str,
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
        where datahora >= '{interval_start}' and datahora < '{interval_end}'
    """
    log("Built query:\n" + query)

    df = pd.read_sql(query, db_url, dtype=str)
    log(f"Query executed successfully. Found {df.shape[0]} rows.")

    if "id" in df.columns:
        log("Detected `id` column in dataframe. Renaming to `gid`", level="warning")
        df.rename(columns={"id": "gid"}, inplace=True)

    now = pd.Timestamp.now(tz="America/Sao_Paulo")
    df["datalake__imported_at"] = now
    log(f"Added `datalake__imported_at` column to dataframe with current timestamp: {now}")

    # Separate dataframe per day for partitioning
    df["partition_date"] = pd.to_datetime(df["datahora"]).dt.date
    days = df["partition_date"].unique()
    dfs = [(day, df[df["partition_date"] == day].drop(columns=["partition_date"])) for day in days]

    output_paths = []
    for day, df_day in dfs:
        file_path = os.path.join(output_file_folder, f"{table_name}_{day}_{uuid.uuid4()}.csv")
        df_day.to_csv(file_path, index=False, header=True, sep=";")
        output_paths.append(file_path)
        log(f"Saving table data to {file_path}")

    return output_paths


@task()
def create_datalake_table_name(table_name: str) -> str:
    return f"{table_name}_eventos"


@task
def get_current_flow_labels() -> list[str]:
    """
    Get the labels of the current flow.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels
