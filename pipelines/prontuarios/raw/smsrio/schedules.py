# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for SMSRio Raw Data Extraction
"""
from datetime import timedelta, datetime
from prefect.schedules import Schedule
import pytz
from pipelines.constants import constants
from pipelines.utils.schedules import (
    untuple_clocks,
    generate_dump_api_schedules
)


#####################################
# SMSRio
#####################################

smsrio_flow_parameters = [
    {
        "environment": "dev"
    }
]

smsrio_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=smsrio_flow_parameters,
    runs_interval_minutes=0,
)

smsrio_daily_update_schedule = Schedule(clocks=untuple_clocks(smsrio_clocks))
