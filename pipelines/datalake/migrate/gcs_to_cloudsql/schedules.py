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
        "environment": "dev",
        "bucket_name": "vitacare_backups_gdrive",
        "instance_name": "vitacare",
        "file_pattern": "HISTÓRICO_PEPVITA_RJ/AP*/vitacare_historic_*_*_*.bak",
    },
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=30),
    start_date=datetime(2025, 5, 7, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=1,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
