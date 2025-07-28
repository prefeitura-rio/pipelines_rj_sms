# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitai Raw Data Extraction
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

routine_flow_parameters = [
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/boletim/listByPeriodo",
        "table_id": "boletim",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-1",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/atendimento/listByPeriodo",
        "table_id": "atendimento",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-1",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/pacientes/listByPeriodo",
        "table_id": "paciente",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-1",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/estabelecimentos/",
        "table_id": "estabelecimento",
        "dataset_id": "brutos_prontuario_vitai_api",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/profissionais/findByEstabelecimento/{estabelecimento_id}",
        "table_id": "profissional",
        "dataset_id": "brutos_prontuario_vitai_api",
    },
    # ------------------------------------------------------------
    # Execuções de Redundância
    # ------------------------------------------------------------
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/boletim/listByPeriodo",
        "table_id": "boletim",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-7",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/atendimento/listByPeriodo",
        "table_id": "atendimento",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-7",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/pacientes/listByPeriodo",
        "table_id": "paciente",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-7",
    },
    # ------------------------------------------------------------
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/boletim/listByPeriodo",
        "table_id": "boletim",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-14",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/atendimento/listByPeriodo",
        "table_id": "atendimento",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-14",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "endpoint_path": "/v1/pacientes/listByPeriodo",
        "table_id": "paciente",
        "dataset_id": "brutos_prontuario_vitai_api",
        "relative_target_date": "D-14",
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
