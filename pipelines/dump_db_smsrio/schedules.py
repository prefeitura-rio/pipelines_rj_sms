# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the smsrio dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.dump_db_smsrio.constants import constants as smsrio_constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "table_id": "estoque",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "dev",
        "rename_flow": True,
    },
    {
        "table_id": "itens_estoque",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "dev",
        "rename_flow": True,
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 12, 15, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

smsrio_daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
