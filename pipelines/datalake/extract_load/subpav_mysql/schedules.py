# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the subpav dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.subpav_mysql.constants import (
    constants as subpav_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "table_id": "bairros",
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_principal",
    },
    {
        "table_id": "competencias",
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_cnes",
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

subpav_weekly_update_schedule = Schedule(clocks=untuple_clocks(clocks))
