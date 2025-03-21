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
        "file_pattern": "AP*/*/REPORT_PLANILHA_ACOMPANHAMENTO_GESTANTES/*/*_SUBPAV_PLAN_ACOMP_MENSAL_GESTANTES.csv",  # noqa: E501
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "acompanhamento_mensal_gestantes",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "AP*/*/REPORT_DISPENSAS_APARELHO_PRESSAO/*/*_DISPENSAS_APARELHO_PRESSAO_*.csv",  # noqa: E501
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "dispensas_aparelho_pressao",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "AP*/*/REPORT_ACOMPANHAMENTO_MULHERES_IDADE_FERTIL/*/*_ACOMPANHAMENTO_MULHERES_IDADE_FERTIL_*.csv",  # noqa: E501
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "acompanhamento_mulheres_idade_fertil",
        "environment": "prod",
        "rename_flow": True,
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 3, 17, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=1,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
