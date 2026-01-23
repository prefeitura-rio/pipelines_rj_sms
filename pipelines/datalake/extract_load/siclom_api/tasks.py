# -*- coding: utf-8 -*-
import json
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
import requests
from google.cloud import bigquery

from pipelines.datalake.extract_load.siclom_api.constants import constants
from pipelines.datalake.extract_load.siclom_api.utils import make_request
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def get_patient_data(environment, batch):
    log("Obtendo dados dos pacientes de interesse...")

    client = bigquery.Client()
    # Primeira versão com 5000 casos para teste. Irá mudar para produção
    sql = """
        select distinct paciente_cpf as cpf
        from `rj-sms.saude_historico_clinico.episodio_assistencial`
        where `condicoes`[SAFE_OFFSET(0)].id  in ('B200','B204','B222','B217','B20','B205','B238','Z21',
                'B211','B213','Z830','B208','Z114','B231','B203','B201','Z206','B212','B221',
                'B24','B209','B220','B219','B210','B230','B207','B21','B22','F024','B232',
                'B23','B206','Z717','R75','B218','B202','B227')
        limit 5000
    """

    df = client.query_and_wait(sql).to_dataframe()
    cpf_list = df["cpf"].to_list()
    chunks = [cpf_list[i : i + batch] for i in range(0, len(cpf_list), batch)]

    return chunks


@task
def fetch_siclom_data(environment, cpf_batch, endpoint, api_key):
    log(f"Buscando dados do Siclom para o lote de CPFs...")

    base_url = constants.BASE_URL.value
    url = f"{base_url}{endpoint}"

    headers = {"Accept": "application/json", "X-API-KEY": api_key}

    patient_informations = []

    for cpf in cpf_batch:
        response = make_request(url=url + cpf, headers=headers)
        if not response:
            continue  # Atualmente a API retorna 500 quando o CPF não é encontrado.
        if response.status_code == 200:
            patient_informations.extend(response.json()["resultado"])

    if patient_informations:
        df = pd.DataFrame(patient_informations)
        df["extracted_at"] = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    else:
        df = pd.DataFrame()

    return df
