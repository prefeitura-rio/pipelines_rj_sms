# -*- coding: utf-8 -*-
"""
Agendamentos
"""

# Geral
from datetime import datetime, time, timedelta

import pytz

# Prefect
from prefect.schedules import Schedule, filters

# Internos
from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

operator_flow_parameters = [
    {
        "environment": "prod",
        "data_inicial": "01/01/2025",
        "data_final": "01/01/2025",
        "bq_dataset": "brutos_siscan_web",
        "bq_table": "laudos",
    }
]


monthly_manager_parameters = [
    {"environment": "prod", "relative_date": "M-1", "range": 1}
]
daily_flow_parameters = [
    {"environment": "prod", "relative_date": "D-1", "range": 1},
]

monthly_manager_clock = generate_dump_api_schedules(
    interval=timedelta(weeks=4),
    start_date=datetime(2025, 8, 6, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=monthly_manager_parameters,
    runs_interval_minutes=0,
)

daily_manager_clock = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 6, 12, 8, 5, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=daily_flow_parameters,
    runs_interval_minutes=0,
)

manager_clocks = daily_manager_clock + monthly_manager_clock

schedule = Schedule(
    clocks=untuple_clocks(manager_clocks), filters=[filters.between_times(time(19), time(23))]
)
