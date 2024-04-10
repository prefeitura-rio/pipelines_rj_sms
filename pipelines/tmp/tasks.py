# -*- coding: utf-8 -*-
import os

import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.tmp.sisreg.constants import constants as sisreg_constants
from pipelines.tmp.sisreg.sisreg import Sisreg
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import get_secret_key


@task
def get_my_ip(environment: str):
    return requests.get("https://api.ipify.org?format=json").json()["ip"]


@task
def get_ip_geolocation(ip: str):
    return requests.get(f"http://ip-api.com/json/{ip}").json()


@task
def log_ip_and_info(ip: str, info: dict):
    log(f"IP: {ip}")
    log(f"Info: {info}")


@task
def extract_escala(environment: str):

    username = get_secret_key.run(
        secret_path=sisreg_constants.INFISICAL_PATH.value,
        secret_name=sisreg_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=environment,
    )
    password = get_secret_key.run(
        secret_path=sisreg_constants.INFISICAL_PATH.value,
        secret_name=sisreg_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=environment,
    )

    sisreg = Sisreg(
        user=username,
        password=password,
        download_path=os.getcwd(),
    )
    sisreg.login()

    return sisreg.download_escala()
