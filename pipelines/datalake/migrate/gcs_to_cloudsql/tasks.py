# -*- coding: utf-8 -*-
import fnmatch
import re
from datetime import datetime, timedelta
from time import sleep

import pytz
import requests
from google.cloud import storage

import pipelines.datalake.migrate.gcs_to_cloudsql.utils as utils
from pipelines.datalake.migrate.gcs_to_cloudsql.constants import constants
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
    #                                             ^^^^^^^ ^^^^^^^^
    #                                              CNES   YYYYMMDD

    regex = re.compile(r"^.+/AP[0-9]+/[a-z_]+_(?P<cnes>[0-9]+)_(?P<date>[0-9]+)_.+$")
    most_recent_dict = {}
    for file in files:
        m = regex.match(file.name)
        date = m.group("date")
        cnes = m.group("cnes")
        if cnes not in most_recent_dict or date > most_recent_dict[cnes][0]:
            most_recent_dict[cnes] = (date, file.name)

    log(f"Found {len(most_recent_dict.keys())} distinct CNES")
    most_recent_filenames = [filename for _, filename in list(most_recent_dict.values())]

    log("Sample of 5 files:\n" + "\n".join(most_recent_filenames[:5]))
    return most_recent_filenames


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def send_sequential_api_requests(most_recent_files: list, bucket_name: str, instance_name: str):
    file_count = len(most_recent_files)
    log(f"[send_sequential_api_requests] Received {file_count} filename(s)")

    # Requisições precisam ser sequenciais porque a API só permite uma operação por vez
    # Caso contrário, dá erro HTTP 409 'Conflict'
    # https://cloud.google.com/sql/docs/troubleshooting#import-export

    # Prepara URL para a API de importação
    PROJECT_ID = constants.PROJECT_ID.value
    API_BASE = f"https://sqladmin.googleapis.com/sql/v1beta4"
    API_PROJECT = f"/projects/{PROJECT_ID}"
    API_INSTANCE = f"/instances/{instance_name}"
    API_IMPORT = f"{API_BASE}{API_PROJECT}{API_INSTANCE}/import"
    # Cabeçalhos da requisição são sempre os mesmos
    access_token = utils.get_access_token()
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}

    for i, file in enumerate(most_recent_files):
        if file is None or len(file) <= 0:
            log(f"[send_sequential_api_requests] Skipping file {i+1}/{file_count} (empty name)")
            continue

        full_file_uri = f"gs://{bucket_name}/{file}"
        log("-" * 20)
        log(
            f"[send_sequential_api_requests] Attempting to import file {i+1}/{file_count}: '{full_file_uri}'"
        )

        # HISTÓRICO.../AP../nome_do_arquivo.bak
        # ------------------^~~~~~~~~~~~~~~~~~~ .rsplit("/")[-1]
        #                   ^~~~~~~~~~~~~~^---- .split(".")[0]
        filename = file.rsplit("/", maxsplit=1)[-1].split(".")[0]
        date_now = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).strftime("%Y%m%d")
        database_name = f"backup_{date_now}__{filename}"
        data = {
            "importContext": {
                "fileType": "BAK",
                "uri": full_file_uri,
                "database": database_name,
                "sqlServerImportOptions": {},
            }
        }
        # Faz a requisição para a API
        response = requests.post(API_IMPORT, json=data, headers=headers)

        # FIXME: Problema em potencial: a database pode já existir, e acredito que isso daria erro
        status = response.status_code
        if status >= 500:
            raise ConnectionError(
                f"[send_sequential_api_requests] Google API responded with status {status}"
            )
        if status >= 400:
            raise PermissionError(
                f"[send_sequential_api_requests] Google API responded with status {status}"
            )

        log(f"[send_sequential_api_requests] Google API responded with status {status}")
        # Resposta é uma instância de 'Operation'
        # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations
        import_json = response.json()

        # "`name` (string): An identifier that uniquely identifies the operation"
        if "name" not in import_json:
            raise KeyError(
                "Google API's JSON response does not contain 'name' Operation identifier"
            )

        name = import_json["name"]
        # Definições para quantas vezes vamos conferir o status
        # Um import que não termine a tempo não necessariamente falhou, e provavelmente
        # vai dar erro 409 Conflict se a gente tentar o seguinte. Mas também não queremos
        # fazer um `while True:` e deixar rodar pra sempre...
        # 200 * 20 = 4000s = ~66.7min = ~1h7min
        MAX_ATTEMPTS = 200
        SLEEP_TIME_SECS = 20
        succeeded = False
        for i in range(MAX_ATTEMPTS):
            # Dorme para não marretar a API
            log(
                f"[send_sequential_api_requests] Sleeping for {SLEEP_TIME_SECS}s ({i+1}/{MAX_ATTEMPTS})..."
            )
            sleep(SLEEP_TIME_SECS)
            # Constrói a URL da API para essa operação
            API_OPERATION = f"/operations/{name}"
            API_POLL = f"{API_BASE}{API_PROJECT}{API_OPERATION}"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            }
            log(
                f"[send_sequential_api_requests] Polling for '{name}' (database '{database_name}')..."
            )
            # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations/get
            response = requests.get(API_POLL, headers=headers)
            poll_json = response.json()
            # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations#sqloperationstatus
            status = "MISSING"
            if "status" in poll_json:
                status = poll_json["status"]
                log(f"[send_sequential_api_requests] Operation status: {status}")
                if status == "DONE":
                    succeeded = True
                    break
            else:
                log(poll_json)
                log(
                    f"[send_sequential_api_requests] 'status' not present in JSON response!",
                    level="warning",
                )

        if not succeeded:
            log(
                f"[send_sequential_api_requests] Operation '{name}' is not done! "
                + "Likely HTTP 409 'Conflict' error coming.",
                level="warning",
            )

    log("[send_sequential_api_requests] All done!")
