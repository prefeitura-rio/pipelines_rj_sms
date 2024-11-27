# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.progress import calculate_operator_key


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
        end_day = datetime(year + 1, 1, 1)
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
    partition_column: str,
    window_size: int = 7,
):
    curr_year = pd.Timestamp.now().year

    params = []
    for year in range(2012, curr_year + 1):
        for window in calculate_windows.run(year=year, window_size=window_size):
            start, end = window
            operator_key = calculate_operator_key.run(
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
                    "partition_column": partition_column,
                    "rename_flow": True,
                }
            )

    return params


@task(max_retries=3, retry_delay=timedelta(seconds=120), timeout=timedelta(minutes=20))
def define_queries(
    db_url: str,
    table_info: dict,
    interval_start: pd.Timestamp,
    interval_end: pd.Timestamp,
    batch_size: int,
) -> str:
    """
    Generates a list of SQL queries to fetch data from a specified table within
        a given time interval in batches.
    Args:
        db_url (str): The database connection URL.
        table_info (dict): A dictionary containing table information with keys:
            - "schema_name": The schema name of the table.
            - "table_name": The name of the table.
            - "datetime_column": The name of the datetime column to filter on.
        interval_start (pd.Timestamp): The start of the time interval.
        interval_end (pd.Timestamp): The end of the time interval.
        batch_size (int): The number of rows to fetch in each batch.
    Returns:
        str: A list of SQL queries to fetch data in batches. If no data is found,
            returns an empty list.
    """
    schema_name = table_info["schema_name"]
    table_name = table_info["table_name"]
    dt_column = table_info["datetime_column"]

    interval_start = interval_start.strftime("%Y-%m-%d %H:%M:%S")
    interval_end = interval_end.strftime("%Y-%m-%d %H:%M:%S")

    query = f"""
        select count(*) as row_count
        from {schema_name}.{table_name}
        where {dt_column} between '{interval_start}' and '{interval_end}'
    """
    log("Built query: \n" + query)

    df = pd.read_sql(query, db_url)

    row_count = df["row_count"].values[0]

    if row_count == 0:
        log("No data found for the given interval", level="warning")
        return []

    queries = []
    for i in range(0, row_count, batch_size):
        queries.append(
            f"""
                select *
                from {schema_name}.{table_name}
                where {dt_column} between '{interval_start}' and '{interval_end}'
                limit {batch_size} offset {i}
            """
        )

    return queries


@task(max_retries=3, retry_delay=timedelta(seconds=120), timeout=timedelta(minutes=5))
def run_query(
    db_url: str,
    query: str,
    partition_column: str,
) -> str:
    log("Running query: \n" + query)

    df = pd.read_sql(query, db_url, dtype=str)
    log(f"Query executed successfully. Found {df.shape[0]} rows.")

    if "id" in df.columns:
        log("Detected `id` column in dataframe. Renaming to `gid`", level="warning")
        df.rename(columns={"id": "gid"}, inplace=True)

    now = pd.Timestamp.now(tz="America/Sao_Paulo")
    df["datalake_loaded_at"] = now
    df["partition_date"] = pd.to_datetime(df[partition_column]).dt.date

    return df
