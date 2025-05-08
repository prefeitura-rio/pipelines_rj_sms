# -*- coding: utf-8 -*-
from google.auth.transport import requests as google_requests
from google.oauth2 import service_account

from pipelines.datalake.migrate.gcs_to_cloudsql.constants import constants
from pipelines.utils.logger import log


def get_access_token(scopes: list=None):
    if scopes is None:
        scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    # Obtém um access token
    credentials = service_account.Credentials.from_service_account_file(
        constants.SERVICE_ACCOUNT_FILE.value, scopes=scopes
    )
    credentials.refresh(google_requests.Request())
    return credentials.token

