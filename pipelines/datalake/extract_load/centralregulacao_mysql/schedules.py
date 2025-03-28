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
from pipelines.datalake.extract_load.centralregulacao_mysql.constants import SCHEMAS
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
        "database": schema,
        "host": SCHEMAS[schema]["host"],
        "port": SCHEMAS[schema]["port"],
        "table": table,
        "query": f"SELECT * FROM {table}",
        "bq_dataset": "brutos_centralderegulacao_mysql",
    }
    for schema in SCHEMAS
    for table in SCHEMAS[schema]["tables"]
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 3, 28, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=3,
)
schedule = Schedule(clocks=untuple_clocks(clocks))
