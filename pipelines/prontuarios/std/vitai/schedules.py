# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitai Data Standardization
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks
from pipelines.prontuarios.utils.tasks import (
    get_flow_scheduled_day
)
#####################################
# Vitai
#####################################


vitai_flow_parameters = [{"environment": "prod",
                           "rename_flow": True}]

vitai_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 4, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=vitai_flow_parameters,
    runs_interval_minutes=0,
)

vitai_std_daily_update_schedule = Schedule(clocks=untuple_clocks(vitai_clocks))
