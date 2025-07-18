# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from typing import Optional

from google.cloud import bigquery
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_bigquery_project_from_environment
from pipelines.utils.time import parse_date_or_today


# Para a justificativa quanto à existência dessa task,
# vide comentários no arquivo de flows
@task
def create_params_dict(environment: str = "dev", date: Optional[str] = None):
    return {"environment": environment, "date": date}


@task
def create_dbt_params_dict(environment: str = "dev"):
    # Queremos executar o seguinte comando:
    # $ dbt build --select +tag:cdi+ --target ENV
    return {
        "environment": environment,
        "rename_flow": True,
        "send_discord_report": False,
        "command": "build",
        "select": "+tag:cdi+",
        "exclude": None,
        "flag": None,
    }


@task
def create_tcm_params_dict(case_id: str, environment: str = "dev"):
    return {
        "environment": environment,
        "case_id": case_id
    }


@task
def fetch_tcm_cases(date: Optional[str]):
    client = bigquery.Client()
    project_name = get_bigquery_project_from_environment.run(environment="prod")

    DATASET = "intermediario_cdi"
    TABLE = "diario_rj_filtrado"
    DATE = parse_date_or_today(date).strftime("%Y-%m-%d")

    QUERY = f"""
SELECT voto
FROM `{project_name}.{DATASET}.{TABLE}`
WHERE voto is not NULL and data_publicacao = '{DATE}'
    """
    log(f"Querying for TCM cases...")
    rows = [row.values()[0] for row in client.query(QUERY).result()]
    log(f"Found {len(rows)} row(s); sample of 5: {rows[:5]}")

    return rows
