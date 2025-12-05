# -*- coding: utf-8 -*-
"""
Agendamentos
"""

# Geral
from datetime import datetime, timedelta

import pytz

# Prefect
from prefect.schedules import Schedule

# Internos
from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "subject": f"E-mail de teste - dia {datetime.date}",
        "recipients": ["matheus.miloski@dados.rio", "juliana.paranhos@regulacaoriorj.com.br"],
        "query_path": "mail_templates/teste/query_teste.sql",
        "html_body_path": "mail_templates/teste/body_teste.html",
        "plain_body_path": "mail_templates/teste/body_texte.txt",
    }
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=7),
    start_date=datetime(2025, 1, 1, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=180,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
