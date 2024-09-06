# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the smsrio dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.smsrio_mysql.constants import (
    constants as smsrio_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "table_id": "estoque",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "schema": "subpav_arboviroses",
    },
    {
        "table_id": "itens_estoque",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "schema": "subpav_arboviroses",
    },
    {
        "table_id": "contatos_equipes",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "schema": "subpav_cnes",
    },
    {
        "table_id": "contatos_unidades",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "schema": "subpav_cnes",
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

smsrio_daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
