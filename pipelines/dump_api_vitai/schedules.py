# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the vitai dump pipeline
"""

from datetime import timedelta, datetime

from prefect.schedules import Schedule
import pytz


from pipelines.constants import constants
from pipelines.dump_api_vitai.constants import (
    constants as vitai_constants,
)
from pipelines.utils.schedules import (
    untuple_clocks,
    generate_dump_api_schedules
)


flow_parameters = [
    {
        "table_id": "estoque_posicao",
        "dataset_id": vitai_constants.DATASET_ID.value,
        "endpoint": "posicao",
        "environment": "prod",
        "rename_flow": True
    },
    {
        "table_id": "estoque_movimento",
        "dataset_id": vitai_constants.DATASET_ID.value,
        "endpoint": "movimento",
        "date": "yesterday",
        "environment": "prod",
        "rename_flow": True
    },
]


vitai_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

vitai_daily_update_schedule = Schedule(clocks=untuple_clocks(vitai_clocks))
