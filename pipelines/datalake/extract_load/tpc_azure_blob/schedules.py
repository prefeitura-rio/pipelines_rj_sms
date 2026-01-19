# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for TPC Raw Data Extraction
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.tpc_azure_blob.constants import (
    constants as tpc_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "blob_file": "posicao",
        "table_id": "estoque_posicao",
        "dataset_id": tpc_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
    },
    # {
    #    "blob_file": "pedidos",
    #    "table_id": "estoque_pedidos_abastecimento",
    #    "dataset_id": tpc_constants.DATASET_ID.value,
    # },
    # {
    #    "blob_file": "recebimento",
    #    "table_id": "estoque_recebimento",
    #    "dataset_id": tpc_constants.DATASET_ID.value,
    # },
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 5, 30, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

tpc_daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
