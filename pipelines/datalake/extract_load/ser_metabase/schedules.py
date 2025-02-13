# -*- coding: utf-8 -*-
"""
Schedules
"""
# Geral
from datetime import datetime, timedelta
import pytz

# Prefect
from prefect.schedules import Schedule

# Internos
from pipelines.constants import constants
from pipelines.datalake.extract_load.ser_metabase.constants import DATABASE_IDS
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = []
for dataset_name, dataset_config in DATABASE_IDS.items():
    db_id = dataset_config["id"]
    for table_name, table_id in dataset_config["tables"].items():
        flow_parameters.append(
            {
                "environment": "prod",
                "database_id": db_id,
                "table_id": table_id,
                "bq_dataset_id": "brutos_ser_metabase",
                "bq_table_id": table_name,
            }
        )

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
