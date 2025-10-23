# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules for the vitai dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

flow_parameters = [
    {
        "environment": "prod",
        "table_name": "paciente",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__paciente_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "boletim",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__boletim_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "alergia",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__alergia_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "atendimento",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__atendimento_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "cirurgia",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__cirurgia_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "classificacao_risco",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__classificacao_risco_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "diagnostico",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__diagnostico_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "exame",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__exame_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "profissional",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__profissional_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "m_estabelecimento",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "basecentral__m_estabelecimento_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "relato_cirurgico",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__relato_cirurgico_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "resumo_alta",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__resumo_alta_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "internacao",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__internacao_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "alta",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__alta_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "fat_boletim",
        "schema_name": "dtw",
        "datetime_column": "datahora",
        "target_name": "dtw__fat_boletim_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "fat_atendimento",
        "schema_name": "dtw",
        "datetime_column": "data_fim",
        "target_name": "dtw__fat_atendimento_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "fat_internacao",
        "schema_name": "dtw",
        "datetime_column": "data_saida",
        "target_name": "dtw__fat_internacao_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "item_prescricao",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__item_prescricao_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
    {
        "environment": "prod",
        "table_name": "prescricao",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__prescricao_eventos",
        "partition_column": "datalake_loaded_at",
        "batch_size": 10000,
    },
]


vitai_clocks = generate_dump_api_schedules(
    interval=timedelta(hours=12),
    start_date=datetime(2024, 11, 12, 2, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

vitai_db_extraction_schedule = Schedule(clocks=untuple_clocks(vitai_clocks))
