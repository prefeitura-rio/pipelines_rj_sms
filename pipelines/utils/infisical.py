# -*- coding: utf-8 -*-
# pylint: disable= C0301
# flake8: noqa: E501
"""
Utilities for infisical.
"""

import base64
import os

from prefeitura_rio.pipelines_utils.infisical import get_infisical_client, inject_env
from prefeitura_rio.pipelines_utils.logging import log


def inject_bd_credentials(environment: str = "dev") -> None:
    """
    Loads Base dos Dados credentials from Infisical into environment variables.

    Args:
        environment (str, optional): The infiscal environment for which to retrieve credentials. Defaults to 'dev'. Accepts 'dev' or 'prod'.

    Returns:
        None
    """
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
    service_account = base64.b64decode(os.environ[service_account_name])

    if not os.path.exists("/tmp"):
        os.makedirs("/tmp")

    with open("/tmp/credentials.json", "wb") as credentials_file:
        credentials_file.write(service_account)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/credentials.json"
