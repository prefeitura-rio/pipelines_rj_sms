import os
import requests
import json
from datetime import datetime

from pipelines.utils.credential_injector import authenticated_task as task


@task
def authenticate_in_metabase(user, password):
    auth_url = "https://metabase.saude.rj.gov.br/api/session"
    auth_payload = {
        "username": user,
        "password": password
    }
    auth_response = requests.post(auth_url, json=auth_payload, verify=False)

    token = auth_response.json()["id"]

    return token

@task
def query_database(token, database_id, table_id):
    query_url = "https://metabase.saude.rj.gov.br/api/dataset"
    headers = {
        "Content-Type": "application/json",
        "X-Metabase-Session": token
    }
    query_payload = {
        "database": database_id,
        "type": "query",
        "parameters": [],
        "query": {
            "source-table": table_id
        }
    }
    query_response = requests.post(query_url, headers=headers, json=query_payload, verify=False)
    json_res = json.dumps(query_response.json())

    return json_res


@task
def save_data(json_res):
    EXTRACTION_DIR = "./extraction/"

    os.makedirs(EXTRACTION_DIR, exist_ok=True)

    with open(f"{EXTRACTION_DIR}{datetime.now().strftime('%y-%m-%d %H_%M_%S')}.json", mode="w") as f:
        f.write(json_res)
