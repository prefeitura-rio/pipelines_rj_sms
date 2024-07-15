# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for upload data from datalake
"""
from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

#####################################
# Vitacare
#####################################

datalake_to_hci_flow_parameters = [
    {
        "environment": "prod",
        "rename_flow": True,
        "table_id": "profissional_saude",
        "dataset_id": "saude_dados_mestres",
        "project_id": "rj-sms",
    },
    {
        "environment": "prod",
        "rename_flow": True,
        "table_id": "equipe_profissional_saude",
        "dataset_id": "saude_dados_mestres",
        "project_id": "rj-sms",
    }
]
datalake_to_hci_clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2024, 1, 1, 6, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=datalake_to_hci_flow_parameters,
    runs_interval_minutes=0,
)

datalake_to_hci_daily_update_schedule = Schedule(clocks=untuple_clocks(datalake_to_hci_clocks))
