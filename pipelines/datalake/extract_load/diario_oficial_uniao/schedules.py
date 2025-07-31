# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

from datetime import datetime, time, timedelta

import pytz
from prefect.schedules import Schedule, filters

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
        "dataset": "brutos_diario_oficial",
        "max_workers": 10,
        "dou_section": 1,
        "date": "",
    },
    {
        "environment": "prod",
        "dataset": "brutos_diario_oficial",
        "max_workers": 10,
        "dou_section": 3,
        "date": "",
    },
]


