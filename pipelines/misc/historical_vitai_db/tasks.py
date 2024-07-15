# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def get_progress_table(slug: str, environment: str):
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")
    table_name = f"rj-sms.gerenciamento__progresso_{slug}.status"

    try:
        bq_client.get_table(table_name)
    except Exception:
        log("Table not found")
        return None

    query = f"""SELECT * FROM {table_name} WHERE environment = '{environment}'"""
    all_records = bq_client.query(query).result().to_dataframe()
    return all_records


@task()
def save_progress(slug: str, environment: str, **kwargs):
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")
    table_name = f"rj-sms.gerenciamento__progresso_{slug}.status"

    table = bq_client.get_table(table_name)

    errors = bq_client.insert_rows_json(
        table,
        [
            {
                "environment": environment,
                "moment": datetime.now().isoformat(),
                **kwargs,
            }
        ],
    )

    if errors == []:
        log("Inserted row")
    else:
        log(f"Errors: {errors}", level="error")
        raise ValueError(f"Errors: {errors}")


@task()
def build_param_list(progress_table, environment: str, table_name: str, window_size: int = 15):
    def calculate_windows(year, window_size):
        # Verifica se o ano é bissexto
        days = 366 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 365

        # Divide o ano em janelas
        windows = []
        ranges = list(range(0, days, window_size))

        for i in range(len(ranges) - 1):
            start_day = datetime(year, 1, 1) + timedelta(days=ranges[i])
            end_day = datetime(year, 1, 1) + timedelta(days=ranges[i + 1]) - timedelta(days=1)
            windows.append((start_day.strftime("%d/%m/%Y"), end_day.strftime("%d/%m/%Y")))

        # Adiciona a última janela
        if ranges[-1] < days:
            start_day = datetime(year, 1, 1) + timedelta(days=ranges[-1])
            end_day = datetime(year, 12, 31)
            windows.append(
                (start_day.strftime("%Y-%m-%d 00:00:00"), end_day.strftime("%Y-%m-%d 00:00:00"))
            )

        return windows

    params = []
    for year in range(2012, 2025):
        for window in calculate_windows(year, window_size):
            start, end = window
            params.append(
                {
                    "interval_start": start,
                    "interval_end": end,
                    "table_name": table_name,
                    "environment": environment,
                    "rename_flow": True,
                }
            )

    candidates = pd.DataFrame(params)
    if progress_table:
        candidates = candidates.merge(
            how="outer", right=progress_table, on=["interval_start", "interval_end"], indicator=True
        )
        remaining = candidates[candidates["_merge"] == "left_only"].to_dict(orient="records")
    else:
        remaining = candidates.to_dict(orient="records")

    return remaining

@task
def to_list(element):
    return [element]