# -*- coding: utf-8 -*-
# pylint: disable=import-error
"""
Schedules para o flow de migração do BigQuery para o MySQL da SUBPAV.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

RJ_TZ = pytz.timezone("America/Sao_Paulo")
LABELS = [constants.RJ_SMS_AGENT_LABEL.value]

TABLES_CONFIG = [
    {
        "project": "SINANRIO - Sintomáticos respiratórios",
        "dataset_id": "projeto_sinanrio",
        "table_id": "sintomaticos_respiratorios_dia",
        "db_schema": "teste_dl",
        "dest_table": "tb_sintomatico",
        "frequency": "daily",
        "if_exists": "append",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
        "custom_insert_query": """
            INSERT INTO teste_dl.tb_sintomatico (
                cpf,
                condicoes,
                soap_subjetivo_motivo,
                soap_objetivo_descricao,
                soap_avaliacao_observacoes,
                datahora_fim,
                nome,
                data_nascimento,
                cids_extraidos,
                rn,
                created_at
            )
            SELECT
                :cpf,
                :condicoes,
                :soap_subjetivo_motivo,
                :soap_objetivo_descricao,
                :soap_avaliacao_observacoes,
                :datahora_fim,
                :nome,
                :data_nascimento,
                :cids_extraidos,
                :rn,
                NOW()
            WHERE NOT EXISTS (
                SELECT 1 FROM teste_dl.tb_sintomaticos
                WHERE cpf = :cpf
                AND DATE(created_at) BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()
            )
        """,
    },
    {
        "project": "SINANRIO - Notificação",
        "dataset_id": "projeto_sinanrio",
        "table_id": "notificacao",
        "db_schema": "teste_dl",
        "dest_table": "notificacao",
        "frequency": "daily",
        "if_exists": "append",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
    },
    {
        "project": "SINANRIO - Tabela de Investigação",
        "dataset_id": "projeto_sinanrio",
        "table_id": "tb_investiga",
        "db_schema": "teste_dl",
        "dest_table": "tb_investiga",
        "frequency": "daily",
        "if_exists": "append",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
    },
    {
        "project": "SINANRIO - Resultado de Exames",
        "dataset_id": "projeto_sinanrio",
        "table_id": "tb_resultado_exame",
        "db_schema": "teste_dl",
        "dest_table": "tb_resultado_exame",
        "frequency": "daily",
        "if_exists": "append",
        "infisical_path": "/plataforma-subpav",
        "notify": True,
    },
]


def build_param(config: dict) -> dict:
    param = {
        "dataset_id": config["dataset_id"],
        "table_id": config["table_id"],
        "db_schema": config["db_schema"],
        "dest_table": config["dest_table"],
        "infisical_path": config["infisical_path"],
        "rename_flow": True,
        "if_exists": config.get("if_exists", "append"),
        "project": config.get("project", ""),
        "environment": config.get("environment", "dev"),
    }
    # Só adiciona se tiver valor!
    if config.get("secret_name") is not None:
        param["secret_name"] = config["secret_name"]
    if config.get("bq_columns") is not None:
        param["bq_columns"] = config["bq_columns"]
    if config.get("custom_insert_query") is not None:
        param["custom_insert_query"] = config["custom_insert_query"]
    if config.get("limit") is not None:
        param["limit"] = config["limit"]
    if config.get("notify") is not None:
        param["notify"] = config["notify"]
    return param


def unpack_params(frequency: str) -> list:
    """
    Retorna uma lista de parâmetros prontos para execução do flow,
    filtrando as configurações de tabela pela frequência informada.
    """
    return [build_param(config) for config in TABLES_CONFIG if config["frequency"] == frequency]


daily_params = unpack_params("daily")
weekly_params = unpack_params("weekly")
monthly_params = unpack_params("monthly")

BASE_START_DATE = datetime(2025, 7, 31, 7, 0, tzinfo=RJ_TZ)

daily_clocks = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=daily_params,
)

weekly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=1),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=weekly_params,
    runs_interval_minutes=1,
)

monthly_clocks = generate_dump_api_schedules(
    interval=timedelta(weeks=4),
    start_date=BASE_START_DATE,
    labels=LABELS,
    flow_run_parameters=monthly_params,
    runs_interval_minutes=1,
)

bq_to_subpav_daily_schedule = Schedule(clocks=untuple_clocks(daily_clocks))
bq_to_subpav_weekly_schedule = Schedule(clocks=untuple_clocks(weekly_clocks))
bq_to_subpav_monthly_schedule = Schedule(clocks=untuple_clocks(monthly_clocks))
bq_to_subpav_combined_schedule = Schedule(
    clocks=untuple_clocks(daily_clocks + weekly_clocks + monthly_clocks)
)
