# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for SMSRio Data Standardization
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.prontuarios.utils.tasks import get_flow_scheduled_day
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

#####################################
# SMSRio
#####################################
start_date = get_flow_scheduled_day()
end_date = start_date + timedelta(days=1)


smsrio_flow_parameters = [
    {
        "environment": "prod",
        "source_start_datetime": start_date,
        "source_end_datetime": end_date,
        "rename_flow": True,
    }
]

smsrio_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 4, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=smsrio_flow_parameters,
    runs_interval_minutes=0,
)

smsrio_std_daily_update_schedule = Schedule(clocks=untuple_clocks(smsrio_clocks))
