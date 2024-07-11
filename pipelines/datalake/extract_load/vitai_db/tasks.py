# -*- coding: utf-8 -*-
import datetime
import os
import shutil
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


@task(nout=2, max_retries=3, retry_delay=datetime.timedelta(seconds=120))
def create_working_time_range(
    project_name: str,
    dataset_name: str,
    table_names: list[str],
    interval_start: str = None,
    interval_end: str = None
) -> list:
    interval_start_values = []
    interval_end_values = []

    seven_days_ago = pd.Timestamp.now(tz="America/Sao_Paulo") - pd.Timedelta(days=7)
    seven_days_ago = seven_days_ago.strftime("%Y-%m-%d")

    if not interval_start:
        log("Interval start not provided. Getting max value from staging tables.", level="warning")
        client = bigquery.Client()

        default_max_value = pd.Timestamp.now(tz="America/Sao_Paulo") - pd.Timedelta(minutes=30)

        interval_start_values = []
        for table_name in table_names:
            query = f"""
            SELECT MAX(datahora) as max_value, "{table_name}" as table_name
            FROM `{project_name}.{dataset_name}_staging.{table_name}`
            WHERE data_particao > '{seven_days_ago}'
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

            interval_start_values.append(max_value)
    else:
        interval_start_values = [pd.to_datetime(
            interval_start, format="%Y-%m-%d %H:%M:%S") for _ in table_names]

    if not interval_end:
        log("Interval end not provided. Getting current timestamp.", level="warning")
        interval_end_values = [pd.Timestamp.now(tz="America/Sao_Paulo") for _ in table_names]
    else:
        interval_end_values = [pd.to_datetime(
            interval_end, format="%Y-%m-%d %H:%M:%S") for _ in table_names]

    for table_name, start, end in zip(table_names, interval_start_values, interval_end_values):
        log(f"Table {table_name} will be extracted from {start} to {end}")

    return interval_start_values, interval_end_values


@task()
def import_vitai_table_to_csv(
    db_url: str,
    table_name: str,
    output_file_folder: str,
    interval_start: pd.Timestamp,
    interval_end: pd.Timestamp,
) -> str:

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
    if df.empty:
        log("Empty dataframe. Preparing to send file with only headers", level="warning")
        dfs = [(str(now.date()), df)]
    else:
        log("Non Empty dataframe. Splitting Dataframe in multiple files by day", level="warning")
        df["partition_date"] = pd.to_datetime(df["datahora"]).dt.date
        days = df["partition_date"].unique()
        dfs = [
            (day, df[df["partition_date"] == day].drop(columns=["partition_date"])) for day in days
        ]  # noqa

    # Save dataframes to csv files
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


@task
def create_folder(title="", subtitle=""):
    folder_path = os.path.join(os.getcwd(), "data", title, subtitle)

    if os.path.exists(folder_path):
        shutil.rmtree(folder_path, ignore_errors=False)
    os.makedirs(folder_path)

    log(f"Created folder: {folder_path}")

    return folder_path
