# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

single_flow_params = [
    {
        "environment": "staging",  #
        "schema": "dbo",
        "rename_flow": False,
    }
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=20),  #
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=single_flow_params,
    runs_interval_minutes=0,
)

vitacare_monthly_schedule = Schedule(clocks=untuple_clocks(clocks))
