# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

from datetime import datetime, timedelta
from typing import List

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {"environment": "prod", "date": None},
]


def get_schedules(times: List[tuple]) -> List[IntervalClock]:
    all_schedules = []
    for hour, minute in times:
        date = datetime(2025, 6, 4, hour, minute, tzinfo=pytz.timezone("America/Sao_Paulo"))
        all_schedules.extend(
            generate_dump_api_schedules(
                interval=timedelta(days=1),
                start_date=date,
                labels=[constants.RJ_SMS_AGENT_LABEL.value],
                flow_run_parameters=flow_parameters,
                runs_interval_minutes=1,
            )
        )
    return all_schedules


# Executamos o flow pela primeira vez às 5:00
# Mas não temos como garantir que o Diário Oficial já terá sido publicado
# Então executamos novamente 5:30, 6:00 e 6:30 para garantir que
# eventualmente pegaremos o DO
clocks = get_schedules([(5, 0), (5, 30), (6, 0), (6, 30)])

schedule = Schedule(clocks=untuple_clocks(clocks))
