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

# Em utils.py, substitua a função inteira por esta:


def call_and_wait(method: str, url_path: str, json=None):
    if not method or len(method) <= 0:
        method = "GET"
    method = method.upper()

    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    log(f"[call_and_wait] {method} {url_path}")
    API_URL = f"{constants.API_BASE.value}{url_path}"

    # ===== INÍCIO DA CORREÇÃO =====
    # Unifica a chamada para garantir que o payload `json` seja sempre enviado,
    # independentemente do método (POST, PATCH, etc.)
    response = requests.request(method, API_URL, headers=headers, json=json)
    # ===== FIM DA CORREÇÃO =====

    status = response.status_code
    log(f"[call_and_wait] API responded with status {status}")

    if status >= 500:
        raise ConnectionError(f"[call_and_wait] API responded with status {status}")

    if status >= 400:
        if method == "DELETE":
            log(
                f"API returned status {status} on DELETE. Assuming database did not exist. Continuing.",
                level="info",
            )
            return

        log(f"API error details: {response.text}", level="error")
        raise PermissionError(f"[call_and_wait] API responded with status {status}")

    res_json = response.json()

    if "name" not in res_json:
        log("[call_and_wait] Response is not an Operation object. No need to wait.", level="info")
        return

    operation_id = res_json["name"]
    sleep(5)

    MAX_ATTEMPTS = 40
    SLEEP_TIME_SECS = 15
    succeeded = False
    for i in range(MAX_ATTEMPTS):
        API_POLL = f"{constants.API_BASE.value}/operations/{operation_id}"
        log(f"[call_and_wait] Polling for '{operation_id}'...")
        poll_response = requests.get(API_POLL, headers=headers)
        poll_json = poll_response.json()

        if (
            "error" in poll_json
            and "errors" in poll_json["error"]
            and len(poll_json["error"]["errors"]) > 0
        ):
            error_count = len(poll_json["error"]["errors"])
            log(
                f"[call_and_wait] API reported {error_count} error(s)",
                level="warning",
            )
            for err in poll_json["error"]["errors"]:
                log(f"{err['code']}: {err['message']}", level="warning")

        if "status" in poll_json:
            op_status = poll_json["status"]
            log(f"[call_and_wait] Operation status: {op_status}")
            if op_status == "DONE":
                succeeded = True
                break
        else:
            log(poll_json, level="warning")
            log(
                "[call_and_wait] 'status' not present in JSON response!",
                level="warning",
            )

        log(f"[call_and_wait] Sleeping for {SLEEP_TIME_SECS}s ({i+1}/{MAX_ATTEMPTS})...")
        sleep(SLEEP_TIME_SECS)

    if not succeeded:
        log(
            f"[call_and_wait] Operation '{operation_id}' is not done! "
            + "Possible HTTP 409 'Conflict' error coming.",
            level="warning",
        )


def get_instance(instance_name: str) -> dict:
    """Busca e retorna o objeto de configuração completo de uma instância."""
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}
    url = f"{constants.API_BASE.value}/instances/{instance_name}"

    log(f"[get_instance] GET {url}")
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Levanta erro se a requisição falhar
    return response.json()
