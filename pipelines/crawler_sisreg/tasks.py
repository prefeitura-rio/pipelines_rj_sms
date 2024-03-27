# -*- coding: utf-8 -*-
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
import os
#from dotenv import load_dotenv
import requests
import json
import google.oauth2.id_token
import google.auth.transport.requests
from prefect.engine.signals import FAIL


@task
def download_data(env='prod') -> None:

    # Ler valores do infisical
    SISREG_USER="user"
    SISREG_PASSWORD="password"

    if env=='prod':
        URL_CLOUD_FUNCTION = 'https://southamerica-east1-rj-sms.cloudfunctions.net/sisreg_crawler'
    else:
        URL_CLOUD_FUNCTION = 'https://southamerica-east1-rj-sms-dev.cloudfunctions.net/sisreg_crawler'

    DOWNLOAD_PATH = "pipelines_rj_sms/pipelines/crawler_sisreg/data/"
    # Lembrar exportar a variavel "export GOOGLE_APPLICATION_CREDENTIALS" com o local da credencial.
    request = google.auth.transport.requests.Request()
    audience = URL_CLOUD_FUNCTION
    TOKEN = google.oauth2.id_token.fetch_id_token(request, audience)
    credential_string = json.dumps({
        "username": SISREG_USER,
        "password": SISREG_PASSWORD
    })

    payload = json.dumps({
        "credential": credential_string
    })
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"}
    response = requests.request("POST", URL_CLOUD_FUNCTION, headers=headers, data=payload)
    if response.status_code == 200:
        # Get file from cloud storage bucket "tmp-sisreg"
        return True
    else:
        raise FAIL(str(f"Error: {response.status_code} - {response.text}"))


