# -*- coding: utf-8 -*-
from datetime import datetime

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants


clock_mensal = CronClock(
    cron="0 20 1 * *",
    start_date=datetime(2025, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    parameter_defaults={
        "environment": "prod",
        "status_desejado": "TODOS_OS_STATUS",
        "data_especifica": None
    }
)


clock_diario = CronClock(
    cron="0 2 * * *", 
    start_date=datetime(2025, 1, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    parameter_defaults={
        "environment": "prod",
        "status_desejado": "TODOS_OS_STATUS",
        "data_especifica": "ontem"
    }
)

schedule = Schedule(clocks=[clock_mensal, clock_diario])


schedule = Schedule(clocks=[clock_mensal])