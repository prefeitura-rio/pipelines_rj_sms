# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants as global_constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

manager_flow_params = [
    {
        "ENVIRONMENT": "prod",
        "DB_SCHEMA_MANAGER": "dbo",
    }
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=20),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=manager_flow_params,
    runs_interval_minutes=0,
)


vitacare_backup_manager_schedule = Schedule(clocks=untuple_clocks(clocks))
