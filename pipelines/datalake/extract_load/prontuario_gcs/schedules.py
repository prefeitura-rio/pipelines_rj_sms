from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks


flow_parameters = [
    {
    "bucket_name": "subhue_backups",
    "chunk_size": 5000,
    "dataset": "brutos_prontuario_prontuaRio_staging",
    "environment": "prod",
    "folder": ""
    } 
]

# 2026-04-06 Foi segunda-feira
clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    start_date=datetime(
        year=2026, 
        month=4, 
        day=6, 
        hour=21, 
        minute=0, 
        tzinfo=pytz.timezone("America/Sao_Paulo")
    ),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))