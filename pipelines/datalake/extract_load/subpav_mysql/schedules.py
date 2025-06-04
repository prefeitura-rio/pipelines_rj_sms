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

# ----------------------
# Parâmetros MENSAIS
# ----------------------
monthly_flow_parameters = [
    {
        "table_id": "bairros",
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_principal",
    },
    {
        "table_id": "indicadores",
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_indicadores",
    },
]

monthly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=4),  # Equivalente a mensal
    start_date=datetime(2025, 6, 1, 6, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=monthly_flow_parameters,
    runs_interval_minutes=0,
)

subpav_monthly_update_schedule = Schedule(clocks=untuple_clocks(monthly_clocks))


# ----------------------
# Parâmetros SEMANAIS
# ----------------------

# CNES APS
tables = [
    "competencias",
    "unidades",
    "horarios_atendimentos",
    "unidades_auxiliares",
    "unidades_motivos_desativacoes",
    "equipes",
    "equipes_tipos",
    "equipes_profissionais",
    "profissionais",
    "profissionais_unidades",
    "categorias_profissionais",
    "cbos",
    "cbos_categorias_profissionais",
]
weekly_flow_parameters = [
    {
        "table_id": tabela,
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_cnes",
    }
    for tabela in tables
]

weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 6, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=weekly_flow_parameters,
    runs_interval_minutes=0,
)


# ----------------------
# Parâmetros POR MINUTO
# ----------------------

# AMS - Acesso Mais Seguro
tables = [
    "consequencias",
    "eventos",
    "locais",
    "notificacoes",
    "notificacoes_consequencias",
    "notificacoes_eventos",
    "notificacoes_protagonistas",
    "protagonistas",
    "riscos",
    "status",
    "notificacoes_equipes",
]
per_minute_flow_parameters = [
    {
        "table_id": tabela,
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_acesso_mais_seguro",
    }
    for tabela in tables
]

per_minute_clocks = generate_dump_api_schedules(
    interval=timedelta(minutes=1),
    start_date=datetime(2025, 6, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=per_minute_flow_parameters,
    runs_interval_minutes=0,
)

subpav_combined_schedule = Schedule(
    clocks=untuple_clocks(monthly_clocks + weekly_clocks + per_minute_clocks)
)
