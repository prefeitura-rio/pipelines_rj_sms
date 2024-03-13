# -*- coding: utf-8 -*-
# pylint: disable=C0103,C0301
"""
Schedules for the smsrio dump url
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "url": "https://docs.google.com/spreadsheets/d/1EkYfxuN2bWD_q4OhHL8hJvbmQKmQKFrk0KLf6D7nKS4/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "Sheet1",
        "table_id": "estabelecimento_auxiliar",
        "dataset_id": "brutos_sheets",
        "csv_delimiter": ";",
        "environment": "dev",
        "rename_flow": True,
    },
    {
        "url": "https://docs.google.com/spreadsheets/d/1PHbpFFSNWNaswecH1Z0tn62p5-2FqMJgjIXknkeGX34/edit?usp=sharing",  # noqa: E501
        "url_type": "google_sheet",
        "gsheets_sheet_name": "CONSOLIDADO",
        "dataset_id": "brutos_sheets",
        "table_id": "material_remume",
        "csv_delimiter": "|",
        "environment": "dev",
        "rename_flow": True,
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 12, 20, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
