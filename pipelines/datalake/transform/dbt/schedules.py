# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the dbt execute pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

daily_parameters = [
    {"command": "run", "environment": "prod", "rename_flow": True},
    {"command": "test", "environment": "prod", "rename_flow": True},
]

weekly_parameters = [
    {"command": "test", "environment": "dev", "rename_flow": True},
]


dbt_daily_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 6, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=daily_parameters,
    runs_interval_minutes=10,
)

dbt_weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2024, 3, 17, 6, 50, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=weekly_parameters,
    runs_interval_minutes=10,
)


dbt_clocks = dbt_daily_clocks + dbt_weekly_clocks

dbt_schedules = Schedule(clocks=untuple_clocks(dbt_clocks))