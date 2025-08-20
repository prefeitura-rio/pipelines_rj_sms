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


def wait_for_operations(instance_name: str, label: str = ""):
    if len(label) > 0:
        label = f"wait_for_operations ({label})"
    else:
        label = "wait_for_operations"

    # Cabeçalhos da requisição são sempre os mesmos
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # Pegamos só 1 (`maxResults=1`) pra saber o status da operação mais recente
    # "reverse chronological order of the start time"
    # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations/list
    url_path = f"/operations?instance={instance_name}&maxResults=1"
    API_URL = f"{constants.API_BASE.value}{url_path}"

    # Definições de quantas vezes vamos conferir o status
    # 40 * 15 = 600s = 10min
    MAX_ATTEMPTS = 40
    SLEEP_TIME_SECS = 15
    succeeded = False
    for i in range(MAX_ATTEMPTS):
        log(f"[{label}] Polling for operations...")
        response = requests.request("GET", API_URL, headers=headers)

        # Confere se requisição foi bem sucedida
        status = response.status_code
        if status != 200:
            log(f"[{label}] API responded with status {status}")
        if status >= 400:
            log(response.content.decode("utf-8"), level="error")
        response.raise_for_status()

        res_json = response.json()
        if "items" not in res_json:
            log(f"[{label}] 'items' field not in API response; skipping check", level="warning")
            return

        operations = res_json["items"]
        if len(operations) <= 0:
            log(f"[{label}] No operations returned!")
            return
        operation = operations[0]

        # Obtém UUID da operação
        operation_id = "(unknown ID)"
        if "name" in operation:
            operation_id = operation["name"]

        # Confere se houve algum erro
        if (
            "error" in operation
            and "errors" in operation["error"]
            and len(operation["error"]["errors"]) > 0
        ):
            # Às vezes os "erros" são na verdade warnings, então não precisa morrer por isso
            error_count = len(operation["error"]["errors"])
            log(
                f"[{label}] API reported {error_count} error(s) for operation '{operation_id}'",
                level="warning",
            )
            for err in operation["error"]["errors"]:
                log(f"{err['code']}: {err['message']}", level="warning")

        # Confere o status atual da operação
        # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1beta4/operations#sqloperationstatus
        status = "?"
        if "status" in operation:
            status = operation["status"]
            log(f"[{label}] Latest operation '{operation_id}' has status {status}")
            if status == "DONE":
                succeeded = True
                break
        else:
            log(operation)
            log(
                f"[{label}] 'status' not present in JSON response for operation '{operation_id}'!",
                level="warning",
            )

        # Última coisa antes de tentar de novo: dorme para não marretar a API
        log(f"[{label}] Sleeping for {SLEEP_TIME_SECS}s ({i+1}/{MAX_ATTEMPTS})...")
        sleep(SLEEP_TIME_SECS)

    if not succeeded:
        log(
            f"[{label}] Max wait attempts reached; " + "Possible HTTP 409 'Conflict' error coming.",
            level="warning",
        )

    # Percebemos que, às vezes, mesmo após um status "DONE"
    # a API responde com 409 Conflict; então, por via das dúvidas,
    # damos mais uma última dormida rápida pra tentar evitar isso
    sleep(10)


def call_api(method: str, url_path: str, json=None):
    if not method or len(method) <= 0:
        method = "GET"
    method = method.upper()

    # Cabeçalhos da requisição são sempre os mesmos
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    log(f"[call_api] {method} {url_path}")
    API_URL = f"{constants.API_BASE.value}{url_path}"

    # Envia a requisição
    if json is not None:
        response = requests.request(method, API_URL, headers=headers, json=json)
    else:
        response = requests.request(method, API_URL, headers=headers)

    # Confere se foi bem sucedida
    status = response.status_code
    if status != 200:
        log(f"[call_api] API responded with status {status}")
    if status >= 400:
        log(response.content.decode("utf-8"), level="error")
    response.raise_for_status()


def get_instance_status(instance_name: str):
    # Cabeçalhos da requisição são sempre os mesmos
    access_token = get_access_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    url_path = f"/instances/{instance_name}"
    log(f"[get_instance_status] GET {url_path}")
    API_URL = f"{constants.API_BASE.value}{url_path}"

    response = requests.request("GET", API_URL, headers=headers)

    # Confere se foi bem sucedida
    status = response.status_code
    if status != 200:
        log(f"[get_instance_status] API responded with status {status}")
    response.raise_for_status()

    # Resposta é (deveria ser) um json; queremos saber o `state`
    res_json = response.json()
    if (
        "state" not in res_json
        or "settings" not in res_json
        or "activationPolicy" not in res_json["settings"]
    ):
        log(
            f"[get_instance_status] Cannot find running state for instance '{instance_name}'",
            level="warning",
        )
        return

    # `RUNNABLE: The instance is running, or has been stopped by owner.`
    # [Ref] https://cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/instances#SqlInstanceState
    state = res_json["state"]
    # ALWAYS (running), NEVER (stopped)
    # [Ref] https://cloud.google.com/sql/docs/postgres/admin-api/rest/v1beta4/instances#sqlactivationpolicy
    activation_policy = res_json["settings"]["activationPolicy"]

    wrong_state = state != "RUNNABLE"
    wrong_policy = activation_policy not in ("ALWAYS", "NEVER")
    log(
        f"[get_instance_status] Instance is in running state '{state}', activation policy '{activation_policy}'",
        level=("warning" if wrong_state or wrong_policy else "info"),
    )
