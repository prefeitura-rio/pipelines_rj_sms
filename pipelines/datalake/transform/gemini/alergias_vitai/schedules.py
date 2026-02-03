# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitai Allergies Standardization
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

#####################################
# Vitai Allergies Standardization
#####################################

vitai_alergias_flow_parameters = [{"environment": "prod", "rename_flow": True}]

vitai_alergias_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 4, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=vitai_alergias_flow_parameters,
    runs_interval_minutes=0,
)

vitai_alergias_daily_update_schedule = Schedule(clocks=untuple_clocks(vitai_alergias_clocks))
