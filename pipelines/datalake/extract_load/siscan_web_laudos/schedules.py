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
        "data_inicial": "01/01/2020",
        "data_final": "now",
        "bq_dataset": "brutos_siscan_web",
        "bq_table": "laudos",
        "dias_por_faixa": 15,
        "formato_data": "%d/%m/%Y",

    }
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
