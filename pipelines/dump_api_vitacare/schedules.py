# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the vitacare dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.dump_api_vitacare.constants import constants as vitacare_constants
from pipelines.dump_api_vitacare.utils.schedules import (
    generate_dicts,
    generate_dump_api_schedules,
)
from pipelines.utils.schedules import untuple_clocks

posicao_parameters = generate_dicts(
    dict_template={
        "environment": "dev",
        "ap": "",
        "cnes": "",
        "endpoint": "posicao",
        "date": "today",
        "dataset_id": vitacare_constants.DATASET_ID.value,
        "table_id": "estoque_posicao",
        "rename_flow": True,
    },
    ap=["10", "21", "22", "31", "32", "33", "40", "51", "52", "53"],
    cnes=vitacare_constants.CNES.value,
)

movimento_parameters = generate_dicts(
    dict_template={
        "environment": "dev",
        "ap": "",
        "cnes": "",
        "endpoint": "movimento",
        "date": "yesterday",
        "dataset_id": vitacare_constants.DATASET_ID.value,
        "table_id": "estoque_movimento",
        "rename_flow": True,
    },
    ap=["10", "21", "22", "31", "32", "33", "40", "51", "52", "53"],
    cnes=vitacare_constants.CNES.value,
)

flow_parameters = posicao_parameters  # + movimento_parameters

vitacare_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 9, 55, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=2,
    parallel_runs=50,
)

vitacare_daily_update_schedule = Schedule(clocks=untuple_clocks(vitacare_clocks))
