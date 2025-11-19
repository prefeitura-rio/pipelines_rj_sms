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

flow_parameters = [
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


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 10, 2, 8, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
