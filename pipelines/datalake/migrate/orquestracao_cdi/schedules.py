# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule, filters

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

parameters = [
    {"environment": "prod"},
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 8, 27, 6, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=parameters,
)

schedules = Schedule(
    clocks=untuple_clocks(clocks),
    # Só executa em dias úteis
    filters=[filters.is_weekday],
)
