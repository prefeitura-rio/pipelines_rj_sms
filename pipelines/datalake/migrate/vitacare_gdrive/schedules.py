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
        "ap": "AP53",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP52",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP51",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP40",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP33",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP32",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP31",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP22",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP21",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "AP10",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
    {
        "ap": "PRISIONAL",
        "environment": "prod",
        "last_modified_date": "M-0",
        "rename_flow": True,
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 3, 17, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=1,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
