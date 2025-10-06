# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for Procedimentos SISREG Standardization
"""
from datetime import datetime, timedelta
import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks
from pipelines.datalake.transform.gemini.procedimentos_sisreg.constants import (
    constants as procedimentos_constants,
)

#####################################
# Procedimentos SISREG Standardization
#####################################

procedimentos_sisreg_flow_parameters = [
    {
        "environment": "prod",
        "rename_flow": True,
        "query": procedimentos_constants.QUERY.value,
    }
]

procedimentos_sisreg_clocks = generate_dump_api_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2025, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=procedimentos_sisreg_flow_parameters,
    runs_interval_minutes=0,
)

procedimentos_monthly_update_schedule = Schedule(
    clocks=untuple_clocks(procedimentos_sisreg_clocks)
)
