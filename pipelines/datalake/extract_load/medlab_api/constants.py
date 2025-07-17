# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for the Medlab Api pipeline
"""

from enum import Enum


class medlab_api_constants(Enum):
    """
    Constant values for the medlab flows
    """

    INFISICAL_PATH = "/medlab"
    INFISICAL_URL = "API_URL"
    INFISICAL_USERNAME = "API_USUARIO"
    INFISICAL_PASSWORD = "API_SENHA"
    INFISICAL_CODACESSO = "API_CODACESSO"
    DATASET_ID = "brutos_medlab"
    GCS_BUCKET_NAME = "medlab_laudos"
