# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for DataSUS Raw Data Extraction
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
        "gcs_bucket": "svs_cie",
        "gcs_path_template": "exames_laboratoriais/gal_lacen/tuberculose/*/*/*.zip",
        "dataset_id": "brutos_gal",
        "table_id": "exames_laboratoriais",
        "relative_target_date": "D-1",
    },
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 8, 1, 8, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=30,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
