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
        "environment": "prod",
        "database": "monitoramento",
        "host": "db.smsrio.org",
        "table": "vw_MS_CadastrosAtivacoesGov",
        "query": "SELECT * FROM vw_MS_CadastrosAtivacoesGov",
        "bq_dataset": "brutos_centralderegulacao_mysql",
    },
    {
        "environment": "prod",
        "database": "monitoramento",
        "host": "db.smsrio.org",
        "table": "vw_fibromialgia_relatorio",
        "query": "SELECT * FROM vw_fibromialgia_relatorio",
        "bq_dataset": "brutos_centralderegulacao_mysql",
    },
    {
        "environment": "prod",
        "database": "monitoramento",
        "host": "db.smsrio.org",
        "table": "vw_minhaSaude_listaUsuario",
        "query": "SELECT * FROM vw_minhaSaude_listaUsuario",
        "bq_dataset": "brutos_centralderegulacao_mysql",
    },
    {
        "environment": "prod",
        "database": "monitoramento",
        "host": "db.smsrio.org",
        "table": "vw_tea_relatorio",
        "query": "SELECT * FROM vw_tea_relatorio",
        "bq_dataset": "brutos_centralderegulacao_mysql",
    },
    {
        "environment": "prod",
        "database": "dw",
        "host": "db.smsrio.org",
        "table": "vw_minhasauderio_pesquisa_satisfacao",
        "query": "SELECT * FROM vw_minhasauderio_pesquisa_satisfacao",
        "bq_dataset": "brutos_centralderegulacao_mysql",
    },
    {
        "environment": "prod",
        "database": "subhue_eletivas",
        "host": "db.smsrio.org",
        "table": "vw_eletivas_sisare",
        "query": "SELECT * FROM vw_eletivas_sisare",
        "bq_dataset": "brutos_centralderegulacao_mysql",
    },
]

clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2025, 3, 28, 0, 1, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=3,
)
schedule = Schedule(clocks=untuple_clocks(clocks))
