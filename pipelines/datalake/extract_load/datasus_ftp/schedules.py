# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for DataSUS Raw Data Extraction
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "dataset_id": "brutos_cnes_web",
        "endpoint": "cnes",
        "environment": "dev",
        "download_newest": True,
        "rename_flow": True,
    },
    {
        "dataset_id": "brutos_datasus",
        "endpoint": "cbo",
        "environment": "dev",
        "download_newest": False,
        "file": "TAB_CNES.zip",
        "rename_flow": True,
    },
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 10, 50, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=30,
)

datasus_weekly_update_schedule = Schedule(clocks=untuple_clocks(clocks))
