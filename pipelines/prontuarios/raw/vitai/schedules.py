# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitai Raw Data Extraction
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz

from pipelines.constants import constants
from pipelines.utils.schedules import (
    untuple_clocks,
    generate_dump_api_schedules
)


#####################################
# Vitai
#####################################

vitai_flow_parameters = [
    {
        "cnes": '5717256',
        "environment": "prod"
    }
]

vitai_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=vitai_flow_parameters,
    runs_interval_minutes=0,
)

vitai_daily_update_schedule = Schedule(clocks=untuple_clocks(vitai_clocks))
