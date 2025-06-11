# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

import pytz
from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

# Executamos o flow pela primeira vez às 5:00
# Mas não temos como garantir que o Diário Oficial já terá sido publicado
# Então executamos novamente 5:30, 6:00 e 6:30 para garantir que
# eventualmente pegaremos o DO
times = [(5, 0), (5, 30), (6, 0), (6, 30)]

flow_parameters = {"environment": "prod"}


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
        for hour, minute in times
    ]
)
