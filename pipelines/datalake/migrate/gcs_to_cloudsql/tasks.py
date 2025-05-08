# -*- coding: utf-8 -*-
import fnmatch
import re
from time import sleep
from datetime import timedelta

import requests
from google.cloud import storage

from pipelines.datalake.migrate.gcs_to_cloudsql.constants import constants
from pipelines.datalake.migrate.gcs_to_cloudsql.utils import get_access_token
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def find_all_files_from_pattern(file_pattern: str, environment: str, bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()

    log(f"Using pattern: {file_pattern}")
    files = []
    for blob in blobs:
        if fnmatch.fnmatch(blob.name, file_pattern):
            files.append(blob)

    log(f"{len(files)} files were found")
    return files


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_most_recent_filenames(files):
    # Aqui não temos como ser muito genéricos; queremos os backups mais recentes
    # de cada CNES. O formato dos nomes é o seguinte (ênfase no importante):
    # HISTÓRICO_PEPVITA_RJ/AP32/vitacare_historic_6808077_20250401_051630.bak
    #                        ^^                   ^^^^^^^ ^^^^^^^^
    #                        AP                    CNES   YYYYMMDD

    regex = re.compile(r"^.+/AP[0-9]+/[a-z_]+_(?P<cnes>[0-9]+)_(?P<date>[0-9]+)_.+$")
    most_recent_dict = {}
    for file in files:
        m = regex.match(file.name)
        date = m.group("date")
        cnes = m.group("cnes")
        if cnes not in most_recent_dict \
        or date > most_recent_dict[cnes][0]:
            most_recent_dict[cnes] = (date, file.name)

    log(f"Found {len(most_recent_dict.keys())} distinct CNES")
    most_recent_filenames = [ filename for _, filename in list(most_recent_dict.values()) ]

    log("Sample of 10 files:\n" + "\n".join(most_recent_filenames[:10]))
    # FIXME: coloquei [:5] pra testar com poucos
    return most_recent_filenames[:5] 


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def send_api_request(most_recent_file: str, bucket_name: str, instance_name: str):
    full_file_uri = f"gs://{bucket_name}/{most_recent_file}"

    # FIXME: coloquei pra testar só com 1
    if most_recent_file != "HISTÓRICO_PEPVITA_RJ/AP10/vitacare_historic_2270250_20250401_050209.bak":
        return
    log(f"Attempting to import '{full_file_uri}'")

    # Prepara requisição para a API de importação
    PROJECT_ID = constants.PROJECT_ID.value
    API_BASE = f"https://sqladmin.googleapis.com/sql/v1beta4"
    API_PROJECT = f"/projects/{PROJECT_ID}"
    API_INSTANCE = f"/instances/{instance_name}"
    API_FULL_URL = f"{API_BASE}{API_PROJECT}{API_INSTANCE}/import"

    # HISTÓRICO.../AP../nome_do_arquivo.bak
    # ------------------^~~~~~~~~~~~~~~~~~~ .rsplit("/")[-1]
    #                   ^~~~~~~~~~~~~~^---- .split(".")[0]
    filename = most_recent_file.rsplit("/", maxsplit=1)[-1].split(".")[0]
    data = {
        "importContext": {
            "fileType": "BAK",
            "uri": full_file_uri,
            "database": f"backup__{filename}",
            "sqlServerImportOptions": {},
        }
    }

    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    response = requests.post(API_FULL_URL, json=data, headers=headers)

    log(response.json())
    status = response.status_code
    if status >= 500:
        raise ConnectionError(f"Google API responded with status {status}")
    if status >= 400:
        raise PermissionError(f"Google API responded with status {status}")

    # Resposta é uma instância de 'Operation'
    # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations
    json = response.json()
    # "`name` (string): An identifier that uniquely identifies the operation"
    if "name" in json:
        return json.name

    # Se chegamos aqui, temos um problema: não deu erro, mas não tem ID da operação
    log(json)
    raise KeyError("Google API's JSON response does not contain 'name' Operation identifier")


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def poll_operations_status(operation_names: list):
    # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations/get

    # Prepara requisição para a API de importação
    PROJECT_ID = constants.PROJECT_ID.value
    API_BASE = f"https://sqladmin.googleapis.com/sql/v1beta4"
    API_PROJECT = f"/projects/{PROJECT_ID}"
    access_token = get_access_token()

    # Queremos pegar o status das operações incompletas de tempos em tempos
    # Salvamos todas em um dicionário e vamos atualizando seus valores
    # conforme forem terminando
    operations = dict()
    for name in operation_names:
        operations[name] = True

    # Definições para quantas vezes vamos conferir o status
    # FIXME: Um import que não termine a tempo não necessariamente falhou. E aí?
    MAX_ATTEMPTS = 50
    SLEEP_TIME_SECS = 60
    not_done = 0
    for _ in range(MAX_ATTEMPTS):
        # Flag para sair desse loop cedo se possível
        not_done = 0
        for name, status in operations:
            # Confere se a operação já tem um status de DONE
            # (o que eu acho que não significa que ela foi bem sucedida?)
            # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations#sqloperationstatus
            if status == "DONE":
                continue
            # Constrói a URL da API para essa operação
            API_OPERATION = f"/operations/{name}"
            API_FULL_URL = f"{API_BASE}{API_PROJECT}{API_OPERATION}"
            headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
            response = requests.get(API_FULL_URL, headers=headers)
            json = response.json()
            if "status" in json:
                operations[name] = json.status
                if json.status != "DONE":
                    not_done += 1
            else:
                not_done += 1
        # Se não tem mais ninguém, terminamos
        if not_done <= 0:
            break
        # Se chegamos aqui, ainda falta alguém; dorme
        sleep(SLEEP_TIME_SECS)

    if not_done > 0:
        log(operations, level="error")
        raise RuntimeError(f"There's still {not_done} operation(s) pending.")

    log("Everything is done!")
    return True
