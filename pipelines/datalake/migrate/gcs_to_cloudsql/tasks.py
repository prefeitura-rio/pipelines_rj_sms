# -*- coding: utf-8 -*-
import fnmatch
from datetime import timedelta

import random
import requests

from google.cloud import storage
from google.auth import jwt
from google.auth.transport import requests as google_requests
from google.oauth2 import service_account

from pipelines.datalake.migrate.gcs_to_cloudsql.constants import constants

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_most_recent_file_from_pattern(file_pattern: str, environment: str, bucket_name: str):
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()
    files = []
    for blob in blobs:
        if fnmatch.fnmatch(blob.name, file_pattern):
            files.append(blob)
    log(f"{len(files)} files were found")

    files.sort(key=lambda x: x.updated)
    most_recent_file = files[-1].name
    log(f"Most recent file: {most_recent_file}")
    return most_recent_file


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def send_api_request(bucket_name: str, most_recent_file: str, instance_name: str):
    full_file_uri = f"gs://{bucket_name}/{most_recent_file}"

    # Obtém um access token
    # https://cloud.google.com/sql/docs/mysql/connect-auth-proxy#create-service-account
    SCOPES = ["https://www.googleapis.com/auth/sqlservice.admin"]
    SERVICE_ACCOUNT_FILE = constants.SERVICE_ACCOUNT_FILE.value
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=SCOPES
    )
    credentials.refresh(google_requests.Request())
    ACCESS_TOKEN = credentials.token

    # Prepara requisição para a API de importação
    PROJECT_ID = constants.PROJECT_ID.value
    API_BASE = f"https://sqladmin.googleapis.com/sql/v1beta4"
    API_PROJECT = f"/projects/{PROJECT_ID}"
    API_INSTANCE = f"/instances/{instance_name}"
    API_FULL_URL = f"{API_BASE}{API_PROJECT}{API_INSTANCE}/import"

    rnd_seed = random.randrange(100_000, 1_000_000)
    data = {
        "importContext": {
            "fileType": "BAK",
            "uri": full_file_uri,
            "database": f"_test_database_{rnd_seed}",
            "sqlServerImportOptions": {}
        }
    }
    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    response = requests.post(API_FULL_URL, json=data, headers=headers)

    log(response.status_code)
    log(response.json())
    # 403 unauthorized :(
