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
from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
        "es_index": "solicitacao-ambulatorial-rj",
        "page_size": 5_000,
        "scroll_timeout": "10m",
        "filters": {"codigo_central_reguladora": "330455"},
        "data_final": "now",
        "bq_dataset": "brutos_sisreg_api",
        "bq_table": "solicitacoes",
        "dias_por_faixa": 5,
        "formato_data": "%Y-%m-%d",
    },
    {
        "environment": "prod",
        "es_index": "marcacao-ambulatorial-rj",
        "page_size": 5_000,
        "scroll_timeout": "10m",
        "filters": {"codigo_central_reguladora": "330455"},
        "data_final": "now",
        "bq_dataset": "brutos_sisreg_api",
        "bq_table": "marcacoes",
        "dias_por_faixa": 5,
        "formato_data": "%Y-%m-%d",
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 1, 1, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=180,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
