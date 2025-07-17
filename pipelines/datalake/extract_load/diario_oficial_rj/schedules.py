# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

flow_parameters = {"environment": "prod"}

# Executamos o flow pela primeira vez às 5:00
# Mas não temos como garantir que o Diário Oficial já terá sido publicado
# Então executamos novamente 5:30, 6:00 e 6:30 para garantir que
# eventualmente pegaremos o DO
TIMES = [(5, 0), (5, 30), (6, 0), (6, 30)]
daily_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=1),
            start_date=datetime(
                2020, 6, 4, hour, minute, tzinfo=pytz.timezone("America/Sao_Paulo")
            ),
            labels=[constants.RJ_SMS_AGENT_LABEL.value],
            parameter_defaults=flow_parameters,
        )
        for hour, minute in TIMES
    ]
)
