# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks


flow_parameters = [
    {
        "id_code": cnes,
        "environment": "prod",
        "rename_flow": True,
        "schema": "dbo",
        "partition_column": "extracted_at",
    }
    for cnes in vitacare_constants.cnes_codes
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=20),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

vitacare_monthly_schedule = Schedule(clocks=untuple_clocks(clocks))