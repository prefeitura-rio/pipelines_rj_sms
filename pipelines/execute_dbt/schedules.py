# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the dbt execute pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz


from pipelines.constants import constants
from pipelines.utils.schedules import (
    untuple_clocks,
    generate_dump_api_schedules
)


flow_parameters = [
    {
        "command": "run",
        "environment": "dev",
        "rename_flow": True
    },
    {
        "command": "test",
        "environment": "dev",
        "rename_flow": True
    },
]


dbt_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 10, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=10,
)

dbt_daily_update_schedule = Schedule(clocks=untuple_clocks(dbt_clocks))
