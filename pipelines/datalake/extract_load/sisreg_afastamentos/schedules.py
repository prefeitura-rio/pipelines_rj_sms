# -*- coding: utf-8 -*-
"""
Agendamentos
"""

# Geral
from datetime import datetime, timedelta

import pytz

# Prefect
from prefect.schedules import Schedule

# Internos
from pipelines.constants import constants as pipeline_constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks
from pipelines.datalake.extract_load.sisreg_afastamentos import constants

flow_parameters = [
    {
        "environment": "prod",
        "dataset_id": constants.DEFAULT_DATASET_ID,
        "afastamento_table_id": constants.DEFAULT_AFASTAMENTO_TABLE_ID,
        "historico_table_id": constants.DEFAULT_HISTORICO_TABLE_ID,
        "min_task_wait": constants.SLEEP_TIME,
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        pipeline_constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=180,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
