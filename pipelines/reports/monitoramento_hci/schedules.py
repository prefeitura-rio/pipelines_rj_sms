# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
    },
]

TIMEZONE = pytz.timezone("America/Sao_Paulo")

clocks = generate_dump_api_schedules(
    interval=timedelta(minutes=30),
    start_date=datetime(2025, 6, 11, 8, 0, tzinfo=TIMEZONE),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(
    clocks=untuple_clocks(clocks),
)
