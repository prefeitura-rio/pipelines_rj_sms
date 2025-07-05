# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the smsrio dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.datalake.extract_load.smsrio_mysql.constants import (
    constants as smsrio_constants,
)
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "table_id": "estoque",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_arboviroses",
    },
    {
        "table_id": "itens_estoque",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_arboviroses",
    },
    {
        "table_id": "contatos_equipes",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_cnes",
    },
    {
        "table_id": "contatos_unidades",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_cnes",
    },
    {
        "table_id": "unidades",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "subpav_cnes",
    },
    {
        "table_id": "tb_preparos_finais",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "mode": "replace",
        "schema": "transparencia",
    },
    {
        "table_id": "tb_pacientes",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "relative_date_filter": "D-7",
        "partition_column": "datalake_loaded_at",
        "schema": "sms_pacientes",
    },
    {
        "table_id": "tb_pacientes_telefones",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "relative_date_filter": "D-7",
        "partition_column": "datalake_loaded_at",
        "schema": "sms_pacientes",
    },
    {
        "table_id": "tb_cns_provisorios",
        "dataset_id": smsrio_constants.DATASET_ID.value,
        "environment": "prod",
        "rename_flow": True,
        "relative_date_filter": "D-7",
        "partition_column": "datalake_loaded_at",
        "schema": "sms_pacientes",
    },
    {
        "dataset_id": "brutos_centralderegulacao_mysql",
        "environment": "prod",
        "infisical_path": "/cr_mysql",
        "partition_column": "datalake_loaded_at",
        "rename_flow": True,
        "schema": "monitoramento",
        "table_id": "vw_MS_CadastrosAtivacoesGov",
    },
    {
        "dataset_id": "brutos_centralderegulacao_mysql",
        "environment": "prod",
        "infisical_path": "/cr_mysql",
        "partition_column": "datalake_loaded_at",
        "rename_flow": True,
        "schema": "monitoramento",
        "table_id": "vw_fibromialgia_relatorio",
    },
    {
        "dataset_id": "brutos_centralderegulacao_mysql",
        "environment": "prod",
        "infisical_path": "/cr_mysql",
        "partition_column": "datalake_loaded_at",
        "rename_flow": True,
        "schema": "monitoramento",
        "table_id": "vw_minhaSaude_listaUsuario",
        "id_column": "idUsuario",
    },
    {
        "dataset_id": "brutos_centralderegulacao_mysql",
        "environment": "prod",
        "infisical_path": "/cr_mysql",
        "partition_column": "datalake_loaded_at",
        "rename_flow": True,
        "schema": "monitoramento",
        "table_id": "vw_tea_relatorio",
    },
    {
        "dataset_id": "brutos_centralderegulacao_mysql",
        "environment": "prod",
        "infisical_path": "/cr_mysql",
        "partition_column": "datalake_loaded_at",
        "rename_flow": True,
        "schema": "dw",
        "id_column": "cpf",
        "table_id": "vw_minhasauderio_pesquisa_satisfacao",
    },
]


clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1, 5, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

smsrio_daily_update_schedule = Schedule(clocks=untuple_clocks(clocks))
