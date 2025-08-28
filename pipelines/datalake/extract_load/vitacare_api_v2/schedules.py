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
    # ------------------------------------------------------------
    # Execuções do Dia
    # ------------------------------------------------------------
    {
        "dataset_id": "brutos_prontuario_vitacare_api",
        "endpoint": "posicao",
        "environment": "prod",
        "rename_flow": True,
        "table_id_prefix": "estoque_posicao",
        "target_date": "D-0",
    },
    {
        "dataset_id": "brutos_prontuario_vitacare_api",
        "endpoint": "movimento",
        "environment": "prod",
        "rename_flow": True,
        "table_id_prefix": "estoque_movimento",
        "target_date": "D-1",
    },
    {
        "dataset_id": "brutos_prontuario_vitacare_api",
        "endpoint": "vacina",
        "environment": "prod",
        "rename_flow": True,
        "table_id_prefix": "vacinacao",
        "target_date": "D-3",
    },
    # ------------------------------------------------------------
    # Execuções de Redundância
    # ------------------------------------------------------------
    {
        "dataset_id": "brutos_prontuario_vitacare_api",
        "endpoint": "movimento",
        "environment": "prod",
        "rename_flow": True,
        "table_id_prefix": "estoque_movimento",
        "target_date": "D-7",
    },
    # ------------------------------------------------------------
    {
        "dataset_id": "brutos_prontuario_vitacare_api_centralizadora",
        "endpoint": "vacina",
        "environment": "prod",
        "rename_flow": True,
        "table_id_prefix": "vacinacao",
        "target_date": "D-7",
    },
]

routine_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 7, 20, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=routine_flow_parameters,
    runs_interval_minutes=30,
)

schedules = Schedule(clocks=untuple_clocks(routine_clocks))
