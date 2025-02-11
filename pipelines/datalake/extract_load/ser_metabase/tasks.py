# -*- coding: utf-8 -*-
import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def authenticate_in_metabase(user, password):
    log("Iniciando autenticação no Metabase. Usuário: {}".format(user))
    auth_url = "https://metabase.saude.rj.gov.br/api/session"
    auth_payload = {"username": user, "password": password}
    auth_response = requests.post(auth_url, json=auth_payload, verify=False)

    token = auth_response.json()["id"]
    log("Autenticação concluída com sucesso.")
    return token


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def query_database(token, database_id, table_id):
    log(
        "Iniciando consulta ao banco de dados. Database ID: {}, Table ID: {}".format(
            database_id, table_id
        )
    )
    query_url = "https://metabase.saude.rj.gov.br/api/dataset"
    headers = {"Content-Type": "application/json", "X-Metabase-Session": token}
    query_payload = {
        "database": database_id,
        "type": "query",
        "parameters": [],
        "query": {"source-table": table_id},
    }
    query_response = requests.post(query_url, headers=headers, json=query_payload, verify=False)
    json_res = json.dumps(query_response.json())
    log("Consulta ao banco de dados concluída.")
    return json_res


@task
def save_data(json_res):
    log("Iniciando salvamento dos dados.")
    EXTRACTION_DIR = "./extraction/"
    os.makedirs(EXTRACTION_DIR, exist_ok=True)

    file_path = f"{EXTRACTION_DIR}{datetime.now().strftime('%y-%m-%d %H_%M_%S')}.json"
    with open(file_path, mode="w") as f:
        f.write(json_res)
    log("Dados salvos em: {}".format(file_path))


@task
def convert_metabase_json_to_df(json_res):
    log("Iniciando conversão de JSON para DataFrame.")
    data_as_dict = json.loads(json_res)
    rows = data_as_dict["data"]["rows"]
    cols_names = [col["name"] for col in data_as_dict["data"]["cols"]]

    df = pd.DataFrame(rows, columns=cols_names)
    df["data_extracao"] = datetime.now()
    log("Conversão concluída.")
    return df
