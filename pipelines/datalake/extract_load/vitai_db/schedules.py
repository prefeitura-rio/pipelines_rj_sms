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
        "target_name": "basecentral__paciente_eventos"
    },
    {
        "environment": "prod",
        "table_name": "boletim",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__boletim_eventos"
    },
    {
        "environment": "prod",
        "table_name": "alergia",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__alergia_eventos"
    },
    {
        "environment": "prod",
        "table_name": "atendimento",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__atendimento_eventos"
    },
    {
        "environment": "prod",
        "table_name": "cirurgia",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__cirurgia_eventos"
    },
    {
        "environment": "prod",
        "table_name": "classificacao_risco",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__classificacao_risco_eventos"
    },
    {
        "environment": "prod",
        "table_name": "diagnostico",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__diagnostico_eventos"
    },
    {
        "environment": "prod",
        "table_name": "exame",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__exame_eventos"
    },
    {
        "environment": "prod",
        "table_name": "profissional",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__profissional_eventos"
    },
    {
        "environment": "prod",
        "table_name": "m_estabelecimento",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__m_estabelecimento_eventos"
    },
    {
        "environment": "prod",
        "table_name": "relato_cirurgico",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__relato_cirurgico_eventos"
    },
    {
        "environment": "prod",
        "table_name": "resumo_alta",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__resumo_alta_eventos"
    },
    {
        "environment": "prod",
        "table_name": "internacao",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__internacao_eventos"
    },
    {
        "environment": "prod",
        "table_name": "alta",
        "schema_name": "basecentral",
        "datetime_column": "created_at",
        "target_name": "basecentral__alta_eventos"
    },
    {
        "environment": "prod",
        "table_name": "fat_boletim",
        "schema_name": "dtw",
        "datetime_column": "data_entrada",
        "target_name": "dtw__fat_boletim_eventos"
    },
    {
        "environment": "prod",
        "table_name": "fat_atendimento",
        "schema_name": "dtw",
        "datetime_column": "data_fim",
        "target_name": "dtw__fat_atendimento_eventos"
    },
    {
        "environment": "prod",
        "table_name": "fat_internacao",
        "schema_name": "dtw",
        "datetime_column": "data_entrada",
        "target_name": "dtw__fat_internacao_eventos"
    },
    # ==============================
    # (TEMP) LÓGICA ANTIGA: Para transição
    # ==============================
    {
        "environment": "prod",
        "table_name": "paciente",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "paciente_eventos"
    },
    {
        "environment": "prod",
        "table_name": "boletim",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "boletim_eventos"
    },
    {
        "environment": "prod",
        "table_name": "alergia",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "alergia_eventos"
    },
    {
        "environment": "prod",
        "table_name": "atendimento",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "atendimento_eventos"
    },
    {
        "environment": "prod",
        "table_name": "cirurgia",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "cirurgia_eventos"
    },
    {
        "environment": "prod",
        "table_name": "classificacao_risco",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "classificacao_risco_eventos"
    },
    {
        "environment": "prod",
        "table_name": "diagnostico",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "diagnostico_eventos"
    },
    {
        "environment": "prod",
        "table_name": "exame",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "exame_eventos"
    },
    {
        "environment": "prod",
        "table_name": "profissional",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "profissional_eventos"
    },
    {
        "environment": "prod",
        "table_name": "m_estabelecimento",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "m_estabelecimento_eventos"
    },
    {
        "environment": "prod",
        "table_name": "relato_cirurgico",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "relato_cirurgico_eventos"
    },
    {
        "environment": "prod",
        "table_name": "resumo_alta",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "resumo_alta_eventos"
    },
    {
        "environment": "prod",
        "table_name": "internacao",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "internacao_eventos"
    },
    {
        "environment": "prod",
        "table_name": "alta",
        "schema_name": "basecentral",
        "datetime_column": "datahora",
        "target_name": "alta_eventos"
    },
]


vitai_clocks = generate_dump_api_schedules(
    interval=timedelta(minutes=30),
    start_date=datetime(2024, 10, 22, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    flow_run_parameters=flow_parameters,
    runs_interval_minutes=0,
)

vitai_db_extraction_schedule = Schedule(clocks=untuple_clocks(vitai_clocks))
