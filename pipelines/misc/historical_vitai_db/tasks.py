# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.progress import calculate_operator_key


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