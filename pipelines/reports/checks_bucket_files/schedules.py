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
        "bucket_name": "cgcca_cnes",
        "source_freshness": "M-0",
    },
    {
        "environment": "prod",
        "bucket_name": "cgcca_sih",
        "source_freshness": "Y-0",
    },
    {
        "environment": "prod",
        "bucket_name": "conectividade_aps",
        "source_freshness": "D-0",
    },
    {
        "environment": "prod",
        "bucket_name": "vitacare_informes_mensais_gdrive",
        "source_freshness": "M-0",
    },
    {
        "environment": "prod",
        "bucket_name": "vitacare_backups_gdrive",
        "source_freshness": "M-0",
    },
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2025, 1, 5, 9, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
