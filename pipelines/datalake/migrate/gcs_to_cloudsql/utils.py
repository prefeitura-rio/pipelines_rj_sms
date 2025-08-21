# -*- coding: utf-8 -*-
import re
from time import sleep

import requests
from google.auth.transport import requests as google_requests
from google.oauth2 import service_account

from pipelines.datalake.migrate.gcs_to_cloudsql.constants import constants
from pipelines.utils.logger import log


def get_access_token(scopes: list = None):
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    # Obtém um access token
    credentials = service_account.Credentials.from_service_account_file(
        constants.SERVICE_ACCOUNT_FILE.value, scopes=scopes
    )
    credentials.refresh(google_requests.Request())
    return credentials.token


def get_info_from_filename(filename: str):
    # Exemplo de nome:
    # "HISTÓRICO_PEPVITA_RJ/AP10/vitacare_historic_2269953_20250301_034009.bak"
    # Às vezes termina com "_old.bak"
    file = filename.strip().rsplit("/", maxsplit=1)[1].lower()
    regex = re.compile(
        r"^(?P<name>[a-z_]+)_(?P<cnes>[0-9]+)_(?P<date>[0-9]{8})_(?P<time>[0-9]{6})(_old)?\.[a-z]+$"
    )
    m = regex.match(file)
    if m is None:
        log(file, level="error")
    return {
        "name": m.group("name"),
        "cnes": m.group("cnes"),
        "date": m.group("date"),
        "time": m.group("time"),
    }


def check_db_name(name: str):
    # Se for um dos nomes reservados (i.e. pré-existentes), erro
    if name in ["master", "model", "msdb", "tempdb"]:
        raise PermissionError(f"Database name '{name}' is reserved!")

    # Se tiver algum caractere esquisito (e.g. *, %), erro
    if re.search(r"[^A-Za-z0-9_\-]", name):
        raise PermissionError(f"Database name '{name}' contains characters not in [A-Za-z0-9_\\-]!")

    return


# Arquivo: utils.py

def call_and_wait(method: str, url_path: str, json=None):
    if not method or len(method) <= 0:
        method = "GET"
    method = method.upper()

    # Cabeçalhos da requisição são sempre os mesmos
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    log(f"[call_and_wait] {method} {url_path}")
    API_URL = f"{constants.API_BASE.value}{url_path}"

    # Envia a requisição
    if method == "POST":
        response = requests.request(method, API_URL, headers=headers, json=json)
    else:
        response = requests.request(method, API_URL, headers=headers)

    # Confere se foi bem sucedida
    status = response.status_code
    log(f"[call_and_wait] API responded with status {status}")

    if status >= 500:
        raise ConnectionError(f"[call_and_wait] API responded with status {status}")

    # ===== INÍCIO DA ALTERAÇÃO =====
    if status >= 400:
        # Para a operação DELETE, um erro 4xx geralmente significa que o banco de dados
        # não foi encontrado, o que é um resultado aceitável para nós.
        if method == "DELETE":
            log(
                f"API returned status {status} on DELETE. Assuming database did not exist. Continuing.",
                level="info",
            )
            return  # Considera a operação um sucesso e sai da função.

        # Para qualquer outro método (como POST), um erro 4xx ainda é um erro real.
        log(f"API error details: {response.text}", level="error")
        raise PermissionError(f"[call_and_wait] API responded with status {status}")
    # ===== FIM DA ALTERAÇÃO =====

    # Resposta é (deveria ser) uma instância de 'Operation'
    # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations
    res_json = response.json()

    # "`name` (string): An identifier that uniquely identifies the operation"
    if "name" not in res_json:
        # Se a resposta não for uma operação (ex: GET em um database), pode não ter 'name'
        # e a função pode terminar aqui, se for o caso.
        log("[call_and_wait] Response is not an Operation object. No need to wait.", level="info")
        return

    # Pega o identificador da operação que precisamos esperar
    operation_id = res_json["name"]

    # Dorme por 5 segundinhos só pra ter uma chance de
    # já estar DONE no primeiro teste
    sleep(5)

    # Definições de quantas vezes vamos conferir o status
    # 40 * 15 = 600s = 10min
    MAX_ATTEMPTS = 40
    SLEEP_TIME_SECS = 15
    succeeded = False
    for i in range(MAX_ATTEMPTS):
        # Constrói a URL da API para essa operação
        API_BASE = constants.API_BASE.value
        API_OPERATION = f"/operations/{operation_id}"
        API_POLL = f"{API_BASE}{API_OPERATION}"

        log(f"[call_and_wait] Polling for '{operation_id}'...")

        # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations/get
        response = requests.get(API_POLL, headers=headers)
        poll_json = response.json()
        # Confere se houve algum erro
        if (
            "error" in poll_json
            and "errors" in poll_json["error"]
            and len(poll_json["error"]["errors"]) > 0
        ):
            # Às vezes os "erros" são na verdade warnings, então não precisa morrer por isso
            error_count = len(poll_json["error"]["errors"])
            log(
                f"[call_and_wait] API reported {error_count} error(s)",
                level="warning",
            )
            for err in poll_json["error"]["errors"]:
                log(f"{err['code']}: {err['message']}", level="warning")

        # Confere o status atual da operação
        # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations#sqloperationstatus
        status = "?"
        if "status" in poll_json:
            status = poll_json["status"]
            log(f"[call_and_wait] Operation status: {status}")
            if status == "DONE":
                succeeded = True
                break
        else:
            log(poll_json)
            log(
                "[call_and_wait] 'status' not present in JSON response!",
                level="warning",
            )

        # Última coisa antes de tentar de novo: dorme para não marretar a API
        log(f"[call_and_wait] Sleeping for {SLEEP_TIME_SECS}s ({i+1}/{MAX_ATTEMPTS})...")
        sleep(SLEEP_TIME_SECS)

    if not succeeded:
        log(
            f"[call_and_wait] Operation '{operation_id}' is not done! "
            + "Possible HTTP 409 'Conflict' error coming.",
            level="warning",
        )