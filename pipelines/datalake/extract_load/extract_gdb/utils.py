# -*- coding: utf-8 -*-
import datetime
import re
import tempfile
from typing import Literal, Optional, Union

import pytz
import requests
from google.cloud import storage
from loguru import logger

from pipelines.utils.tasks import get_secret_key

from . import shared
from .constants import constants as flow_constants


def get_bearer_token(environment: str = "dev") -> str:
    # Se já temos um token salvo
    if "token" in shared.token and "expires" in shared.token:
        # Verifica se o token ainda está ativo
        expires_in = shared.token["expires"]
        now = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        # Se sim, retorna ele mesmo
        if now < expires_in:
            logger.info("Reusing previous access token")
            return shared.token["token"]
        logger.info("Access token expired; obtaining new one")
    # Se não, precisamos obter um token novo

    username = get_secret_key.run(
        environment=environment, secret_name="USERNAME", secret_path="gdb-extractor"
    )
    password = get_secret_key.run(
        environment=environment, secret_name="PASSWORD", secret_path="gdb-extractor"
    )

    base_url = flow_constants.API_URL.value
    resp = requests.post(f"{base_url}/token", data={"username": username, "password": password})
    resp.raise_for_status()
    json = resp.json()
    if "access_token" not in json:
        logger.info(json)
        raise ValueError("No access token!")

    # Tempo de expiração previsto para o token subtraído de 30s
    seconds_left = max(0, json["expires_in"] - 30)
    logger.info(f"Access token obtained; expires in {(seconds_left/60):.1f} min")
    # Salva data/hora de expiração
    shared.token["expires"] = datetime.datetime.now(
        tz=pytz.timezone("America/Sao_Paulo")
    ) + datetime.timedelta(seconds=seconds_left)
    # Pega token e salva para reuso
    token = json["access_token"]
    shared.token["token"] = token
    return token


def authenticated_request(
    method: Union[Literal["GET"], Literal["POST"]],
    endpoint: str,
    json: Optional[dict],
    enviroment: str = "dev",
) -> dict:
    token = get_bearer_token(environment=enviroment)

    base_url = flow_constants.API_URL.value
    resp = requests.request(
        method, f"{base_url}{endpoint}/", json=json, headers={"Authorization": f"Bearer {token}"}
    )
    # Possível que o token salvo seja inválido (ex. servidor reiniciou no meio tempo)
    if resp.status_code == 401:
        # Limpa 'cache'
        shared.token = dict()
        # Obtém novo token
        token = get_bearer_token(environment=enviroment)
        # Tenta requisição mais uma vez
        resp = requests.request(
            method,
            f"{base_url}{endpoint}/",
            json=json,
            headers={"Authorization": f"Bearer {token}"},
        )
    resp.raise_for_status()
    return resp.json()


def authenticated_post(endpoint: str, json: dict, enviroment: str = "dev") -> dict:
    return authenticated_request("POST", endpoint, json, enviroment=enviroment)


def authenticated_get(endpoint: str, enviroment: str = "dev") -> dict:
    return authenticated_request("GET", endpoint, None, enviroment=enviroment)


def inverse_exponential_backoff(iteration: int, initial: int, base: int, minimum: int):
    return max(initial / (base**iteration), minimum)


def download_gcs_to_file(gcs_uri: str):
    # gcs_uri deve ser algo como "gs://bucket/path/to/your/file.txt"
    path_parts = gcs_uri.removeprefix("gs://").split("/", maxsplit=1)
    bucket_name = path_parts[0]
    blob_name = path_parts[1] if len(path_parts) > 1 else ""
    if not len(blob_name):
        raise ValueError(f"Invalid URI (no file path): '{gcs_uri}'")

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    file_hdl = tempfile.TemporaryFile()
    blob.download_to_file(file_hdl)
    return file_hdl


def format_reference_date(refdate: str | None, uri: str) -> str:
    # Se não há data especificada
    if refdate is None or not refdate:
        # Tentamos pegar do nome do arquivo
        # Os arquivos normalmente terminam ou com MMYYYY ou YYYYMM

        # gs://path/to/file.name.GDB -> [ gs://path/to, file.name.GDB ][1]
        # -> file.name.GDB -> [file.name, GDB][0] -> file.name
        filename = uri.rsplit("/", maxsplit=1)[1].rsplit(".", maxsplit=1)[0]
        # Se o arquivo termina com 6 dígitos
        if re.search(r"[0-9]{6}$", filename):
            # Pegamos somente os dígitos
            dt = filename[-6:]
            # Se os dois primeiros são > 12, então a data começa com ano
            if int(dt[:2]) > 12:
                # [YYYY]MM
                year = dt[:4]
                # YYYY[MM]
                month = dt[4:]
            # Senão, começa com <= 12, e não estamos em um ano <1200
            # Então o mês vem primeiro
            else:
                # [MM]YYYY
                month = dt[:2]
                # MM[YYYY]
                year = dt[2:]

            # TODO: ajustar até 12/dez/2099
            if 2000 < int(year) < 2100 and 0 < int(month) < 13:
                return f"{year}-{month}-01"
        # Aqui, o arquivo não termina com mês e ano; não temos
        # ideia de qual data ele se refere, e não recebemos
        # `refdate` como parâmetro. Então a melhor opção é a
        # data atual :/
        dt = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        return dt.strftime("%Y-%m-01")

    # Aqui, recebemos data de referência! Uhules
    refdate = str(refdate).strip()
    # YYYY-MM?-DD?
    if re.search(r"^[0-9]{4}$", refdate):
        return f"{refdate}-01-01"
    if re.search(r"^[0-9]{4}[\-\/][0-9]{2}$", refdate):
        return f"{refdate}-01".replace("/", "-")
    if re.search(r"^[0-9]{4}[\-\/][0-9]{2}[\-\/][0-9]{2}$", refdate):
        return f"{refdate}".replace("/", "-")
    raise ValueError(f"Expected reference date format YYYY(-MM(-DD)?)?; got '{refdate}'")
