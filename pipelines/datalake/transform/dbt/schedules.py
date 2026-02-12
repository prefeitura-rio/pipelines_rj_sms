# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the dbt execute pipeline
"""

from datetime import datetime, timedelta, time

import pytz
from prefect.schedules import Schedule, filters

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

daily_parameters = [
    {"command": "build", "environment": "prod", "rename_flow": True, "select": "tag:daily"},
    {"command": "source freshness", "environment": "prod", "rename_flow": True},
]

weekly_parameters = [
    {"command": "build", "environment": "prod", "rename_flow": True, "select": "tag:weekly"},
]

monthly_parameters = [
    {"command": "build", "environment": "prod", "rename_flow": True, "select": "tag:monthly"},
]

every_30_minutes_parameters = [
    {
        "command": "build",
        "environment": "prod",
        "rename_flow": True,
        "select": "tag:alerta_doencas",
        "send_discord_report": False,
    },
]

every_4_hours_parameters = [
    {
        "command": "build",
        "environment": "prod",
        "rename_flow": True,
        "select": "tag:cdi-4hours",
        "send_discord_report": False,
    },
]


dbt_daily_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 6, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=daily_parameters,
    runs_interval_minutes=15,
)

dbt_weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2024, 3, 17, 6, 20, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=weekly_parameters,
    runs_interval_minutes=30,
)

dbt_monthly_clocks = generate_dump_api_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2025, 11, 15, 6, 20, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=monthly_parameters,
    runs_interval_minutes=30,
)

dbt_every_30_minutes_clocks = generate_dump_api_schedules(
    interval=timedelta(minutes=30),
    start_date=datetime(2024, 12, 18, 10, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=every_30_minutes_parameters,
    runs_interval_minutes=0,
)
# schedule 4 hours
dbt_every_4_hours_clocks = generate_dump_api_schedules(
    interval=timedelta(hours=4),
    start_date=datetime(2024, 12, 18, 9, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=every_4_hours_parameters,
    runs_interval_minutes=0,
)

dbt_clocks = dbt_daily_clocks + dbt_weekly_clocks + dbt_monthly_clocks + dbt_every_30_minutes_clocks + dbt_every_4_hours_clocks

dbt_schedules = Schedule(clocks=untuple_clocks(dbt_clocks))