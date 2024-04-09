# -*- coding: utf-8 -*-
import requests
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log


@task
def get_my_ip():
    return requests.get("https://api.ipify.org?format=json").json()["ip"]


@task
def get_ip_geolocation(ip: str):
    return requests.get(f"http://ip-api.com/json/{ip}").json()


@task
def log_ip_and_info(ip: str, info: dict):
    log(f"IP: {ip}")
    log(f"Info: {info}")
