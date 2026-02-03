# -*- coding: utf-8 -*-
# pylint: disable= C0301
# flake8: noqa: E501
"""
Utilities for infisical.
"""

import base64
import json
import os
from typing import List

from google.oauth2 import service_account
from prefeitura_rio.pipelines_utils.infisical import get_infisical_client, inject_env
from prefeitura_rio.pipelines_utils.logging import log


def inject_all_secrets(environment: str = "dev") -> dict:
    """
    Injects all secrets from Infisical as environment variables.

    Args:
        environment (str, optional): The infiscal environment for which to retrieve credentials. Defaults to 'dev'. Accepts 'dev' or 'prod'.

    Returns:
        dict: A dictionary containing all secrets.
    """
    client = get_infisical_client()
    secrets = client.get_all_secrets(environment=environment, attach_to_os_environ=True)
    return secrets


def inject_bd_credentials(environment: str = "dev", force_injection=False) -> None:
    """
    Loads Base dos Dados credentials from Infisical into environment variables.

    Args:
        environment (str, optional): The infiscal environment for which to retrieve credentials. Defaults to 'dev'. Accepts 'dev' or 'prod'.

    Returns:
        None
    """
    # Verify if all environment variables are already set
    all_variables_set = True
    for variable in [
        "BASEDOSDADOS_CONFIG",
        "BASEDOSDADOS_CREDENTIALS_PROD",
        "BASEDOSDADOS_CREDENTIALS_STAGING",
        "GOOGLE_APPLICATION_CREDENTIALS",
    ]:
        if not os.environ.get(variable):
            all_variables_set = False
            break

    # If all variables are set, skip injection
    if all_variables_set and not force_injection:
        # log("All environment variables are already set. Skipping injection.")
        return

    # Else inject the variables
    client = get_infisical_client()

    log(f"ENVIROMENT: {environment}")
    for secret_name in [
        "BASEDOSDADOS_CONFIG",
        "BASEDOSDADOS_CREDENTIALS_PROD",
        "BASEDOSDADOS_CREDENTIALS_STAGING",
    ]:
        inject_env(
            secret_name=secret_name,
            environment=environment,
            client=client,
        )

    # Create service account file for Google Cloud
    service_account_name = "BASEDOSDADOS_CREDENTIALS_PROD"
    credentials = base64.b64decode(os.environ[service_account_name])

    if not os.path.exists("/tmp"):
        os.makedirs("/tmp")

    with open("/tmp/credentials.json", "wb") as credentials_file:
        credentials_file.write(credentials)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"


def get_credentials_from_env(scopes: List[str] = None) -> service_account.Credentials:
    """
    Gets credentials from env vars
    """

    env: str = os.getenv("BASEDOSDADOS_CREDENTIALS_PROD", "")
    if env == "":
        raise ValueError("BASEDOSDADOS_CREDENTIALS_PROD env var not set!")
    info: dict = json.loads(base64.b64decode(env))
    cred: service_account.Credentials = service_account.Credentials.from_service_account_info(info)
    if scopes:
        cred = cred.with_scopes(scopes)
    return cred
