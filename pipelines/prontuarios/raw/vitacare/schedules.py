# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Vitacare Raw Data Extraction
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

#####################################
# Parameters
#####################################

ENABLED_ENTITY_VALUES = ["diagnostico"]
ENABLED_CNES_VALUES = ["5621801"]


vitacare_flow_parameters = []
for entity in ENABLED_ENTITY_VALUES:
    for cnes in ENABLED_CNES_VALUES:
        vitacare_flow_parameters.append(
            {
                "cnes": cnes,
                "entity": entity,
                "environment": "prod",
                "rename_flow": True,
            }
        )

#####################################
# Schedules
#####################################

vitacare_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=vitacare_flow_parameters,
    runs_interval_minutes=0,
)

vitacare_daily_update_schedule = Schedule(clocks=untuple_clocks(vitacare_clocks))
