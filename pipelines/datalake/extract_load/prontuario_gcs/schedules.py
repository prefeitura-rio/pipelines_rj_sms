from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks


flow_parameters = {
    "bucket_name": "subhue_backups",
    "chunk_size": 5000,
    "dataset": "brutos_prontuario_prontuaRio_staging",
    "environment": "prod",
    "folder": ""
} 

clock = CronClock(
    cron= "0 22 * * 0", # Todo domingo às 22h
    start_date=datetime(
        year=2026, 
        month=4, 
        day=6, 
        hour=22, 
        minute=0, 
        tzinfo=pytz.timezone("America/Sao_Paulo")
    ),
    parameter_defaults=flow_parameters,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)

schedule = Schedule(clocks=[clock])