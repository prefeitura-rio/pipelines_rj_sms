# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa: E501
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
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_PLANILHA_ACOMPANHAMENTO_GESTANTES/*/*_SUBPAV_PLAN_ACOMP_MENSAL_GESTANTES.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "acompanhamento_mensal_gestantes",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_DISPENSAS_APARELHO_PRESSAO/*/*_DISPENSAS_APARELHO_PRESSAO_*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "dispensas_aparelho_pressao",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_ACOMPANHAMENTO_MULHERES_IDADE_FERTIL/*/*_ACOMPANHAMENTO_MULHERES_IDADE_FERTIL_*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "acompanhamento_mulheres_idade_fertil",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_LISTAGEM_VACINA_V2/*/*_LISTAGEM_VACINA_V2_*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "listagem_vacina_v2",
        "environment": "prod",
        "rename_flow": True,
    },
    # FICHA A
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_SUB_PAV_FICHA_A/*/*_*_RELACAO_FICHA_A_VITAhisCARE.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "ficha_a_vitahiscare",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_SUB_PAV_FICHA_A_V2/*/*SUB_PAV_FICHA_A_V2*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "ficha_a_v2",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_SUB_PAV_FICHA_C_CRIANCA_V2/*/*FICHA_C_CRIANCAS_CAP_V2*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "ficha_c_v2",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_SUB_PAV_RELACAO_CRIANCAS_MENORES_DE_5_ANOS/*/*_RELACAO_CRIANCAS_VITAhisCARE.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "criancas_menores_5_anos",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_INDICADORES_VARIAVEL_3_G*/*/*INDICADORES_VARIAVEL_3_G*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "indicadores_cg_variavel_3",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_INDICADORES_VARIAVEL_2/*/*INDICADORES_VARIAVEL_2*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "indicadores_cg_variavel_2",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_INDICADORES_VARIAVEL_1/*/*INDICADORES_VARIAVEL_1*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "indicadores_cg_variavel_1",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_PLANILHA_ACOMPANHAMENTO_ESTADO_NUTRICIONAL_SISVAN/*/*ESTADO_NUTRICIONAL_SISVAN.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "estado_nutricional_sisvan",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/2025-*/REPORT_LISTAGEM_ATENDIMENTOS_PACIENTES_TEA/*/*_LISTAGEM_ATENDIMENTOS_PACIENTES_TEA*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "listagem_pacientes_tea",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_SOLICITACAO_SAUDE_BUCAL_*/*/*SOLICITACAO_SAUDE_BUCAL_*.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "solicitacao_saude_bucal",
        "environment": "prod",
        "rename_flow": True,
    },
    {
        "file_pattern": "INFORMES-MENSAIS-ETSN/AP*/*/REPORT_SUB_PAV_OFICIO54_RELACAO_HAS_DIA/*/*_RELACAO_HASDM_VITAhisCARE.csv",
        "desired_dataset_name": "brutos_informes_vitacare",
        "desired_table_name": "relacao_hasdm_vitahiscare",
        "environment": "prod",
        "rename_flow": True,
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=datetime(2025, 5, 21, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=1,
)

schedule = Schedule(clocks=untuple_clocks(clocks))
