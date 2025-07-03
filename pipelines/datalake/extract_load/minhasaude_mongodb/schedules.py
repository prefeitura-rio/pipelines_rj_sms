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
from pipelines.datalake.extract_load.minhasaude_mongodb.constants import COLLECTIONS
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "flow_name": "MinhaSaude.rio MongoDB",
        "flow_owner": "1184846547242995722",
        "environment": "prod",
        "host": "db.smsrio.org",
        "port": 27017,
        "authsource": "minhasauderio",
        "database": "minhasauderio",
        "collection": collection,
        "query": {},
        "bq_dataset_id": "brutos_minhasaude_mongodb",
        "bq_table_id": collection,
        "slice_var": props["slice_var"],
        "slice_size": props["slice_size"],
    }
    for collection, props in COLLECTIONS.items()
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=3,
)
schedule = Schedule(clocks=untuple_clocks(clocks))
