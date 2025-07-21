# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

import json
from datetime import datetime
from typing import List, Optional

import pytz
import requests
from google.cloud import bigquery
from prefect.engine.signals import FAIL

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
    return {"environment": environment, "case_id": case_id}


@task
def fetch_tcm_cases(date: Optional[str]) -> List[str]:
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


@task
def build_email(date: Optional[str]) -> str:
    client = bigquery.Client()
    project_name = get_bigquery_project_from_environment.run(environment="prod")

    DATASET = "projeto_cdi"
    TABLE = "email"
    DATE = parse_date_or_today(date).strftime("%Y-%m-%d")
    TODAY = (
        datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        .replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        .strftime("%Y-%m-%d")
    )

    QUERY = f"""
SELECT fonte, content_email, voto
FROM `{project_name}.{DATASET}.{TABLE}`
WHERE data_publicacao = '{DATE}'
    """
    log(f"Querying for email contents for '{DATE}'...")
    rows = [row.values() for row in client.query(QUERY).result()]
    log(f"Found {len(rows)} row(s)")

    # Pegamos todos os processos do TCM relevantes
    tcm_case_numbers = []
    for _, _, voto in rows:
        voto: str
        if not voto or len(voto) <= 0:
            continue
        tcm_case_numbers.append(voto.strip())

    # Se tivemos algum processo relevante
    tcm_cases = dict()
    if len(tcm_case_numbers) > 0:
        # Pega os votos, se tivermos essa informação
        TCM_DATASET = "brutos_diario_oficial_staging"
        TCM_TABLE = "processos_tcm"
        TCM_CASES = ", ".join([f"'{case}'" for case in tcm_case_numbers])
        TCM_QUERY = f"""
SELECT processo_id, decisao_data, voto_conselheiro
FROM `{project_name}.{TCM_DATASET}.{TCM_TABLE}`
WHERE processo_id in ({TCM_CASES})
    and data_particao = '{TODAY}'
        """
        log(f"Querying for {len(tcm_case_numbers)} TCM case decision(s)...")
        tcm_rows = [row.values() for row in client.query(TCM_QUERY).result()]
        log(f"Found {len(tcm_rows)} row(s)")
        # Salva data e URL do voto, mapeado pelo ID
        for id, data, voto in tcm_rows:
            tcm_cases[id] = (data, voto)

    # Constrói cada bloco do email
    email_blocks = {}
    for row in rows:
        fonte, content_email, voto = row
        if fonte not in email_blocks:
            email_blocks[fonte] = []

        content = content_email
        if voto and voto in tcm_cases:
            (vote_date, vote_url) = tcm_cases[voto]
            content += f"\nVoto em {vote_date}: {vote_url}"

        email_blocks[fonte].append(content)

    final_email_string = f"--- [Você Precisa Saber · {DATE}] ---"
    for header, body in email_blocks.items():
        final_email_string += f"[{header}]"
        for content in body:
            final_email_string += f"{content}\n\n"
        final_email_string += "---\n\n"

    timestamp = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%H:%M:%S %d/%m/%Y")
    final_email_string += f"Email gerado em: {timestamp}\n"
    return final_email_string


@task
def send_email(api_base_url: str, token: str, message: str):
    request_headers = {"x-api-key": token}
    request_body = json.dumps(
        {
            "to_addresses": ["matheus.avellar@dados.rio"],
            "cc_addresses": [],  # ["pedro.marques@dados.rio", "vitoria.leite@dados.rio"],
            "bcc_addresses": [],
            "subject": "Você Precisa Saber -- teste",
            "body": message,
            "is_html_body": False,
        }
    )

    if api_base_url.endswith("/"):
        api_base_url = api_base_url.rstrip("/?#")
    endpoint = api_base_url + "/data/mailman"

    response = requests.request("POST", endpoint, headers=request_headers, json=request_body)
    response.raise_for_status()
    # [Ref] https://stackoverflow.com/a/52615216/4824627
    response.encoding = response.apparent_encoding
    resp_json = response.json()
    if "success" in resp_json and resp_json["success"]:
        log("Email delivery requested successfully")
        return

    raise FAIL(f"Email delivery failed: {resp_json}")
