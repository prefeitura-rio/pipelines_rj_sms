# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Agendamentos para extração e carga do SISREG.
"""

# bibliotecas padrão
from datetime import datetime, timedelta
import pytz

# bibliotecas do prefect
from prefect.schedules import Schedule

# módulos internos
from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks


flow_parameters = [
    {
        "dataset_id": "brutos_sisreg_v2",
        "environment": "prod",
        "table_id": "oferta_programada",
    },
]


sisreg_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1, 5, 50, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_VERTEX_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=30,
)

sisreg_daily_update_schedule = Schedule(clocks=untuple_clocks(sisreg_clocks))
