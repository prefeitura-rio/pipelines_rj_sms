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

RJ_TZ = pytz.timezone("America/Sao_Paulo")
LABELS = [constants.RJ_SMS_AGENT_LABEL.value]

# -----------
# Parâmetros
# -----------

# -------- Configuração simples: apenas tabela, schema e frequência
TABELAS_CONFIG = [
    # Principal
    ("bairros", "subpav_principal", "monthly"),
    # Indicadores
    ("indicadores", "subpav_indicadores", "monthly"),
    # CNES APS
    ("competencias", "subpav_cnes", "weekly"),
    ("unidades", "subpav_cnes", "weekly"),
    ("horarios_atendimentos", "subpav_cnes", "weekly"),
    ("unidades_auxiliares", "subpav_cnes", "weekly"),
    ("equipes", "subpav_cnes", "weekly"),
    ("equipes_profissionais", "subpav_cnes", "weekly"),
    ("equipes_tipos", "subpav_cnes", "monthly"),
    ("equipes_profissionais", "subpav_cnes", "weekly"),
    ("profissionais", "subpav_cnes", "weekly"),
    ("profissionais_unidades", "subpav_cnes", "weekly"),
    ("categorias_profissionais", "subpav_cnes", "monthly"),
    ("cbos", "subpav_cnes", "monthly"),
    ("cbos_categorias_profissionais", "subpav_cnes", "monthly"),
    # Acesso Mais Seguro AMS
    ("consequencias", "subpav_acesso_mais_seguro", "monthly"),
    ("eventos", "subpav_acesso_mais_seguro", "monthly"),
    ("locais", "subpav_acesso_mais_seguro", "monthly"),
    ("notificacoes", "subpav_acesso_mais_seguro", "per_minute"),
    ("notificacoes_consequencias", "subpav_acesso_mais_seguro", "per_minute"),
    ("notificacoes_equipes", "subpav_acesso_mais_seguro", "per_minute"),
    ("notificacoes_eventos", "subpav_acesso_mais_seguro", "per_minute"),
    ("notificacoes_protagonistas", "subpav_acesso_mais_seguro", "per_minute"),
    ("protagonistas", "subpav_acesso_mais_seguro", "monthly"),
    ("riscos", "subpav_acesso_mais_seguro", "monthly"),
    ("status", "subpav_acesso_mais_seguro", "monthly"),
]


# -------- Função para gerar parâmetros padrão
def build_param(table_id, schema):
    return {
        "table_id": table_id,
        "schema": schema,
        "dataset_id": subpav_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
    }


# -------- Agrupando por frequência
monthly_params = [build_param(t, s) for t, s, f in TABELAS_CONFIG if f == "monthly"]
weekly_params = [build_param(t, s) for t, s, f in TABELAS_CONFIG if f == "weekly"]
per_minute_params = [build_param(t, s) for t, s, f in TABELAS_CONFIG if f == "per_minute"]

# -------- Gerando os clocks
monthly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=4),
    start_date=datetime(2025, 6, 1, 6, 0, tzinfo=RJ_TZ),
    labels=LABELS,
    flow_run_parameters=monthly_params,
    runs_interval_minutes=1,
)

weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 6, 1, 5, 0, tzinfo=RJ_TZ),
    labels=LABELS,
    flow_run_parameters=weekly_params,
    runs_interval_minutes=1,
)

per_minute_clocks = generate_dump_api_schedules(
    interval=timedelta(minutes=5),
    start_date=datetime(2025, 6, 1, 5, 0, tzinfo=RJ_TZ),
    labels=LABELS,
    flow_run_parameters=per_minute_params,
    runs_interval_minutes=1,
)

# -------- Schedules
subpav_monthly_schedule = Schedule(clocks=untuple_clocks(monthly_clocks))
subpav_weekly_schedule = Schedule(clocks=untuple_clocks(weekly_clocks))
subpav_per_minute_schedule = Schedule(clocks=untuple_clocks(per_minute_clocks))
subpav_combined_schedule = Schedule(
    clocks=untuple_clocks(monthly_clocks + weekly_clocks + per_minute_clocks)
)
