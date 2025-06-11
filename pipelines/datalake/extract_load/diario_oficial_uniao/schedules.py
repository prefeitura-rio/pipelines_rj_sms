# -*- coding: utf-8 -*-
import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {"environment": "prod", "dataset": "brutos_diario_oficial", "max_workers": 10, "dou_section": 1}
]
