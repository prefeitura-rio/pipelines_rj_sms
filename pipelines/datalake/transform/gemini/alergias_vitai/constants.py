# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Vitai Allergies Standardization
"""
from enum import Enum


class constants(Enum):

    INFISICAL_PATH = "/models-rj-sms"
    INFISICAL_API_URL = "URL"
    INFISICAL_API_USERNAME = "USERNAME"
    INFISICAL_API_PASSWORD = "PASSWORD"
    DATASET_ID = "intermediario_historico_clinico"
    TABLE_ID = "alergias_vitai_padronizacao"
    PROJECT = {"dev": "rj-sms-dev", "prod": "rj-sms"}
