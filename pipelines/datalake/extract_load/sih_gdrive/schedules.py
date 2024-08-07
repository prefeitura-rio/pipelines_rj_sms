# -*- coding: utf-8 -*-
# pylint: disable=C0103,C0301
"""
Schedules for the sih dump
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "dataset_id": "brutos_sih",
        "table_id": "indicadores_hospitalares",
        "environment": "prod",
        "rename_flow": True,
    }
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 0, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=3,
)

daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
