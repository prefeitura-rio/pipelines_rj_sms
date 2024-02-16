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
        "environment": "prod",
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
        "environment": "prod",
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

update_parameters = posicao_parameters + movimento_parameters

update_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 4, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=update_parameters,
    runs_interval_minutes=2,
    parallel_runs=10,
)

vitacare_daily_update_schedule = Schedule(clocks=untuple_clocks(update_clocks))


reprocess_parameters = [
    {
        "dataset_id": "brutos_prontuario_vitacare",
        "endpoint": "movimento",
        "environment": "prod",
        "parallel_runs": 20,
        "rename_flow": True,
        "table_id": "estoque_movimento",
    }
]

reprocess_clock = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=reprocess_parameters,
    runs_interval_minutes=1,
)

vitacare_daily_reprocess_schedule = Schedule(clocks=untuple_clocks(reprocess_clock))
