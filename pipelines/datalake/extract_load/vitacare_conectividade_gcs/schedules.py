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
        "source_bucket_name": "conectividade_aps",
        "source_file_prefix": "",
        "source_file_suffix": ".json",
        "destination_table_name": "vitacare",
        "destination_dataset_name": "brutos_conectividade",
        "environment": "prod",
    }
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
