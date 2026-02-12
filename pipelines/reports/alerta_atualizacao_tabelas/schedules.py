# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

freshness_tables_flow_parameters = [
    {
        "environment": "prod",
        "table_ids": {
            "rj-sms.app_historico_clinico.episodio_assistencial": ["DIT - HCI"],
            "rj-sms.app_historico_clinico.paciente": ["DIT - HCI"],
            "rj-sms.projeto_gestacoes.linha_tempo": ["S/SUBPAV/SAP - BI de Gestantes"],
            "rj-sms.projeto_gestacoes.encaminhamentos": ["S/SUBPAV/SAP - BI de Gestantes"],
            "rj-sms.projeto_estoque.estoque_posicao_atual": ["DIT - BI de Farmácia"],
            "rj-sms.projeto_whatsapp.telefones_validos": ["IplanRio - E Aí?"],
        },
    }
]

freshness_hci_flow_parameters = [
    {
        "environment": "prod",
    }
]


freshness_tables_clock = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 10, 2, 8, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=freshness_tables_flow_parameters,
    runs_interval_minutes=0,
)

freshness_hci_clock = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 10, 2, 8, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=freshness_hci_flow_parameters,
    runs_interval_minutes=0,
)

freshness_tables_schedule = Schedule(clocks=untuple_clocks(freshness_tables_clock))
freshness_hci_schedule = Schedule(clocks=untuple_clocks(freshness_hci_clock))