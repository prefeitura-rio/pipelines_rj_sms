# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Schedules para o flow de migração do BigQuery para o MySQL da SUBPAV.

Organiza as tabelas por frequência (diária, semanal e mensal), define os parâmetros
necessários por tabela e gera os agendamentos (clocks) do Prefect.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule

from pipelines.constants import constants
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks

# Timezone padrão do Rio de Janeiro
RJ_TZ = pytz.timezone("America/Sao_Paulo")
LABELS = [constants.RJ_SMS_AGENT_LABEL.value]

# -------------------------------
# Tabelas configuradas para o flow
# -------------------------------
TABELAS_CONFIG = [
    {
        "project_id": "rj-sms-dev",
        "dataset_id": "projeto_subpav",
        "table_id": "subpav_sinanrio__sintomaticos_respiratorios",
        "db_schema": "teste_dl",
        "dest_table": "tb_sintomaticos",
        "frequency": "daily",  # Opções: "daily", "weekly", "monthly"
        "if_exists": "append",
        "infisical_path": "/smsrio",
        "secret_name": "DB_URL",
        "custom_insert_query": """
            INSERT INTO tb_sintomaticos (cpf, cns, created_at, nome, idade,origem)
            SELECT %(cpf)s, %(cns)s, NOW(), %(nome)s, %(idade)s, 'P'
            WHERE NOT EXISTS (
                SELECT 1 FROM tb_sintomaticos
                WHERE (cpf = %(cpf)s OR cns = %(cns)s)
                AND DATE(created_at) BETWEEN DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND CURDATE()
            );
        """,
    },
]


# -------------------------------
# Funções auxiliares
# -------------------------------
def build_param(config: dict) -> dict:
    """
    Constrói o dicionário de parâmetros para execução do flow.
    """
    return {
        "dataset_id": config["dataset_id"],
        "table_id": config["table_id"],
        "db_schema": config["db_schema"],
        "project_id": config["project_id"],
        "dest_table": config["dest_table"],
        "infisical_path": config["infisical_path"],
        "secret_name": config["secret_name"],
        "rename_flow": True,
        "if_exists": config.get("if_exists", "append"),
        "custom_insert_query": config.get("custom_insert_query"),
    }


def unpack_params(frequency: str) -> list[dict]:
    """
    Filtra e gera os parâmetros de acordo com a frequência desejada.
    """
    return [build_param(config) for config in TABELAS_CONFIG if config["frequency"] == frequency]


# -------------------------------
# Parâmetros por frequência
# -------------------------------
daily_params = unpack_params("daily")
weekly_params = unpack_params("weekly")
monthly_params = unpack_params("monthly")

# -------------------------------
# Clocks por frequência
# -------------------------------
BASE_START_DATE = datetime(2025, 7, 21, 3, 0, tzinfo=RJ_TZ)

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

# -------------------------------
# Schedules exportados
# -------------------------------
bq_to_subpav_daily_schedule = Schedule(clocks=untuple_clocks(daily_clocks))
bq_to_subpav_weekly_schedule = Schedule(clocks=untuple_clocks(weekly_clocks))
bq_to_subpav_monthly_schedule = Schedule(clocks=untuple_clocks(monthly_clocks))
bq_to_subpav_combined_schedule = Schedule(
    clocks=untuple_clocks(daily_clocks + weekly_clocks + monthly_clocks)
)
