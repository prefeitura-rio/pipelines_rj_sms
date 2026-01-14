# -*- coding: utf-8 -*-
# pylint: disable=C0103,C0301
"""
Schedule for the Security Report project
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

daily_parameters = [
    {
        "environment": "prod",
        # "date": (não passamos porque queremos 'ontem'),
        # "override_recipients": (não passamos por motivos óbvios)
    },
]

daily_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 10, 10, 7, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=daily_parameters,
)

schedule = Schedule(clocks=untuple_clocks(daily_clocks))
