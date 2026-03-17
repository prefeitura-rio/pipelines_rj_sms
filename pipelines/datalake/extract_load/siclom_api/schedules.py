from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

# Atualmente só para referência
flow_parameters = [
    {
        "environment": "prod",
        "endpoint": "/mostracargacd4periodo/",
        "annual": False,
        "year": None,
        "month": None,
        "extraction_range": None,
        "dataset_id": "brutos_siclom_api",
        "table_id": "carga_cd4",
    },
    {
        "environment": "prod",
        "endpoint": "/mostracargaviralperiodo/",
        "annual": False,
        "year": None,
        "month": None,
        "extraction_range": None,
        "dataset_id": "brutos_siclom_api",
        "table_id": "carga_viral",
    },
    {
        "environment": "prod",
        "endpoint": "/tratamentoperiodo/",
        "annual": False,
        "year": None,
        "month": None,
        "extraction_range": None,
        "dataset_id": "brutos_siclom_api",
        "table_id": "tratamento",
    },
    {
        "environment": "prod",
        "endpoint": "/resultadopepperiodo/",
        "annual": False,
        "year": None,
        "month": None,
        "extraction_range": None,
        "dataset_id": "brutos_siclom_api",
        "table_id": "pep",
    },
    {
        "environment": "prod",
        "endpoint": "/resultadoprepperiodo/",
        "annual": False,
        "year": None,
        "month": None,
        "extraction_range": None,
        "dataset_id": "brutos_siclom_api",
        "table_id": "prep",
    },
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    # 2026-03-21 é um sábado
    start_date=datetime(
        year=2026, month=3, day=21, hour=3, minute=0, tzinfo=pytz.timezone("America/Sao_Paulo")
    ),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=2,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
