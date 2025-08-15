# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitacare Raw Data Extraction
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

routine_flow_parameters = [
    {
        "dataset_id": "brutos_prontuario_vitacare",
        "endpoint": "movimento",
        "environment": "prod",
        "rename_flow": True,
        "table_id": "estoque_movimento",
        "target_date": "yesterday",
    },
    {
        "dataset_id": "brutos_prontuario_vitacare",
        "endpoint": "posicao",
        "environment": "prod",
        "rename_flow": True,
        "table_id": "estoque_posicao",
        "target_date": "today",
    },
    {
        "dataset_id": "brutos_prontuario_vitacare",
        "endpoint": "vacina",
        "environment": "prod",
        "rename_flow": True,
        "table_id": "vacina",
        "target_date": "d-3",
    },
]

routine_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 3, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=routine_flow_parameters,
    runs_interval_minutes=60,
)


reprocess_flow_parameters = [
    {
        "dataset_id": "brutos_prontuario_vitacare",
        "endpoint": "movimento",
        "environment": "prod",
        "rename_flow": True,
        "is_routine": False,
        "table_id": "estoque_movimento",
    },
    {
        "dataset_id": "brutos_prontuario_vitacare",
        "endpoint": "vacina",
        "environment": "prod",
        "rename_flow": True,
        "is_routine": False,
        "table_id": "vacina",
    },
]

reprocess_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 12, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=reprocess_flow_parameters,
    runs_interval_minutes=60,
)

vitacare_clocks = routine_clocks + reprocess_clocks

vitacare_daily_update_schedule = Schedule(clocks=untuple_clocks(vitacare_clocks))
