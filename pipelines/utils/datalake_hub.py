# -*- coding: utf-8 -*-
import json
from datetime import timedelta

import pandas as pd
import requests

from pipelines.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import get_secret_key


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def authenticate(environment: str) -> str:
    api_url = get_secret_key.run(
        secret_path=constants.DATALAKE_HUB_PATH.value,
        secret_name=constants.DATALAKE_HUB_API_URL.value,
        environment=environment,
    )
    api_username = get_secret_key.run(
        secret_path=constants.DATALAKE_HUB_PATH.value,
        secret_name=constants.DATALAKE_HUB_API_USERNAME.value,
        environment=environment,
    )
    api_password = get_secret_key.run(
        secret_path=constants.DATALAKE_HUB_PATH.value,
        secret_name=constants.DATALAKE_HUB_API_PASSWORD.value,
        environment=environment,
    )
    response = requests.post(
        url=f"{api_url}auth/token",
        timeout=180,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "username": api_username,
            "password": api_password,
        },
    )

    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Error getting API token ({response.status_code}) - {response.json()}")


@task
def load_asset(dataframe: pd.DataFrame, asset_id: str, environment: str):
    api_url = get_secret_key.run(
        secret_path=constants.DATALAKE_HUB_PATH.value,
        secret_name=constants.DATALAKE_HUB_API_URL.value,
        environment=environment,
    )
    token = authenticate.run(environment=environment)

    # Convert all columns to string
    dataframe = dataframe.astype(str)

    dataframe.to_json(open("./records.json", "w+"), orient="records")
    records = json.load(open("./records.json", "r"))

    response = requests.post(
        url=f"{api_url}write/{asset_id}",
        timeout=500,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json=records,
    )
    report = response.json()

    if response.status_code != 201:
        raise Exception(f"Error loading asset to Datalake Hub ({response.status_code}) - {report}")

    return report
