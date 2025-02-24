# -*- coding: utf-8 -*-
"""
Tasks
"""
import io

# Geral
import json
from datetime import datetime, timedelta

import pandas as pd
import requests

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message


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
    url = "https://metabase.saude.rj.gov.br/api/dataset/csv"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/x-www-form-urlencoded"}

    dataset_query = {
        "type": "query",
        "database": database_id,
        "query": {"source-table": table_id},
        "parameters": [],
    }

    form_data = {"query": json.dumps(dataset_query, ensure_ascii=False)}

    response = requests.post(url, headers=headers, data=form_data, verify=False)
    csv_file = io.StringIO(response.content.decode("utf-8"))

    df = pd.read_csv(csv_file)
    df["data_extracao"] = datetime.now()

    log("Consulta ao banco de dados concluída.")

    return df


@task
def interrupt_if_empty(df):
    if df.empty:
        send_message(
            title="❌ Erro no Flow SER - METABASE",
            message="O Data Frame está vazio. @matheusmiloski",
            monitor_slug="warning",
        )
        raise Exception("O Data Frame está vazio.")
    return df


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
