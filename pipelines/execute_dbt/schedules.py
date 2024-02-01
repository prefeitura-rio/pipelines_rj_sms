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

flow_parameters = [
    {"command": "run", "environment": "prod", "rename_flow": True},
    {"command": "test", "environment": "prod", "rename_flow": True},
]


dbt_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 6, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=10,
)

dbt_daily_update_schedule = Schedule(clocks=untuple_clocks(dbt_clocks))
