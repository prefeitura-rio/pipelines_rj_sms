# -*- coding: utf-8 -*-
# pylint: disable=C0103

"""
Schedules for medlab api
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants 
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_params = [
    {
        "ENVIRONMENT": "prod",
        "DB_SCHEMA_MANAGER": "dbo",
        "RENAME_FLOW_MANAGER": True,
    }
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=20),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=flow_params,
    runs_interval_minutes=0,
)


medlab_api_schedule = Schedule(clocks=untuple_clocks(clocks))

