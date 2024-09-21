# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for farmácia digital - livro de medicamentos controlados
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "data_competencia": "atual",
        "environment": "prod",
        "rename_flow": True,
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2024, 9, 23, 7, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

weekly_schedule = Schedule(clocks=untuple_clocks(clocks))
