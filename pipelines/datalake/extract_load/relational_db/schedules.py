# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the smsrio dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
        "rename_flow": True,
        "db_url_infisical_key": "PRONTUARIOS_DB_URL",
        "db_url_infisical_path": "/",
        "source_schema_name": "public",
        "source_table_name": "userhistory",
        "source_datetime_column": "timestamp",
        "target_dataset_id": "brutos_aplicacao_hci",
        "target_table_id": "userhistory",
        "relative_datetime": "D-1",
        "historical_mode": True,
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "db_url_infisical_key": "PRONTUARIOS_DB_URL",
        "db_url_infisical_path": "/",
        "source_schema_name": "public",
        "source_table_name": "user",
        "target_dataset_id": "brutos_aplicacao_hci",
        "target_table_id": "user",
        "historical_mode": True,
    },
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

schedules = Schedule(clocks=untuple_clocks(clocks))
