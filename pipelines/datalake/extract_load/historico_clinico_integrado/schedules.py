# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the smsrio dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "dataset_id": "brutos_hci_app",
        "schema": "postgres",
        "table_id": "userhistory",
        "historical_mode": False,
        "environment": "prod",
        "rename_flow": True,
        "reference_datetime_column": "timestamp",
    },
    {
        "dataset_id": "brutos_hci_app",
        "schema": "postgres",
        "table_id": "user",
        "historical_mode": True,
        "environment": "prod",
        "rename_flow": True,
        "reference_datetime_column": "updated_at",
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

hci_daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
