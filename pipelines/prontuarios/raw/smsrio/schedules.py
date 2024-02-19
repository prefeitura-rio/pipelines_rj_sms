# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for SMSRio Raw Data Extraction
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

#####################################
# SMSRio
#####################################

smsrio_flow_parameters = [{"environment": "prod", "rename_flow": True}]

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
