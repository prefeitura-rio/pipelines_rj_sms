# -*- coding: utf-8 -*-
# pylint: disable=C0103

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_manager_parameters = [
    {
        "environment": "prod",
        "intervalo": "D-1",   
        "dataset": "brutos_exames_laboratoriais",  
        "hours_per_window": 2,
        "end_date": None,
    }
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 11, 25, 4, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_manager_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))