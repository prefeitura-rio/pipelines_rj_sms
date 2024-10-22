# -*- coding: utf-8 -*-
import os
import shutil
import uuid
import pandas as pd
from datetime import datetime, timedelta

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.progress import calculate_operator_key
from pipelines.utils.logger import log

from pipelines.datalake.utils.data_transformations import convert_to_parquet


@task(nout=2)
def create_working_time_range(
    interval_start: str = None,
    interval_end: str = None,
) -> list:

    if not interval_start:
        start = pd.Timestamp.now(tz="America/Sao_Paulo") - pd.Timedelta(days=7)
        log(f"Interval start not provided. Using {start}.", level="warning")
    else:
        start = pd.to_datetime(interval_start)

    if not interval_end:
        end = pd.Timestamp.now(tz="America/Sao_Paulo")
        log("Interval end not provided. Getting current timestamp.", level="warning")
    else:
        end = pd.to_datetime(interval_end)

    return start, end


@task()
def calculate_windows(year, window_size):
    # Verifica se o ano é bissexto
    days = 366 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 365

    # Divide o ano em janelas
    windows = []
    ranges = list(range(0, days, window_size))

    for i in range(len(ranges) - 1):
        start_day = datetime(year, 1, 1) + timedelta(days=ranges[i])
        end_day = datetime(year, 1, 1) + timedelta(days=ranges[i + 1])
        windows.append(
            (start_day.strftime("%Y-%m-%d 00:00:00"), end_day.strftime("%Y-%m-%d 00:00:00"))
        )

    # Adiciona a última janela
    if ranges[-1] < days:
        start_day = datetime(year, 1, 1) + timedelta(days=ranges[-1])
        end_day = datetime(year, 12, 31)
        windows.append(
            (start_day.strftime("%Y-%m-%d 00:00:00"), end_day.strftime("%Y-%m-%d 00:00:00"))
        )

    return windows


@task()
def build_param_list(
    environment: str, 
    schema_name: str,
    table_name: str,
    target_name: str,
    datetime_column: str, 
    window_size: int = 7
):

    params = []
    for year in range(2012, 2025):
        for window in calculate_windows.run(year=year, window_size=window_size):
            start, end = window
            operator_key = calculate_operator_key(
                schema_name=schema_name,
                table_name=table_name,
                target_name=target_name,
                interval_start=start,
                window_size=window_size,
            )
            params.append(
                {
                    "operator_key": operator_key,
                    "interval_start": start,
                    "interval_end": end,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "datetime_column": datetime_column,
                    "target_name": target_name,
                    "environment": environment,
                    "rename_flow": True,
                }
            )

    return params


@task(max_retries=3, retry_delay=timedelta(seconds=120), timeout=timedelta(minutes=20))
def load_data_from_vitai_table(
    db_url: str,
    table_info: dict,
    output_file_folder: str,
    interval_start: pd.Timestamp,
    interval_end: pd.Timestamp,
) -> str:
    schema_name = table_info['schema_name']
    table_name = table_info['table_name']
    dt_column = table_info['datetime_column']

    interval_start = interval_start.strftime("%Y-%m-%d %H:%M:%S")
    interval_end = interval_end.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        select *
        from {schema_name}.{table_name}
        where {dt_column} between '{interval_start}' and '{interval_end}'
    """
    log("Built query: \n" + query)
    try:
        df = pd.read_sql(query, db_url, dtype=str)
        log(f"Query executed successfully. Found {df.shape[0]} rows.")
    except Exception as e:
        log(f"Error executing query: {e}", level="warning")
        return []

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
        df["partition_date"] = pd.to_datetime(df[dt_column]).dt.date
        days = df["partition_date"].unique()
        dfs = [
            (day, df[df["partition_date"] == day].drop(columns=["partition_date"])) for day in days
        ]  # noqa

    # Save dataframes to csv files
    output_paths = []
    for day, df_day in dfs:
        file_path = os.path.join(output_file_folder, f"{table_name}_{day}_{uuid.uuid4()}.csv")
        df_day.to_csv(file_path, index=False, header=True, sep=";")
        log(f"Saving CSV table data to {file_path}")

        file_path_parquet = convert_to_parquet(file_path=file_path, file_type="csv")
        log(f"Converted table data to parquet in {file_path_parquet}")
        output_paths.append(file_path_parquet)

    return output_paths
