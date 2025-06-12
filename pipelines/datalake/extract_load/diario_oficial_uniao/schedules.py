# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
        "dataset": "brutos_diario_oficial",
        "max_workers": 10,
        "dou_section": 1,
    },
    {
        "environment": "prod",
        "dataset": "brutos_diario_oficial",
        "max_workers": 10,
        "dou_section": 2,
    },
    {
        "environment": "prod",
        "dataset": "brutos_diario_oficial",
        "max_workers": 10,
        "dou_section": 3,
    },
]

# Atos oficiais podem ser publicados no DOU até as 8 a.m de cada dia.
clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 6, 12, 8, 5, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
