# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Datalake Data Extraction and Upload to HCI
"""
from enum import Enum


class constants(Enum):
    TABLE_ID = 'profissional_saude'
    DATASET_ID = 'saude_dados_mestres'
    PROJECT_ID = 'rj-sms-dev'
    INFISICAL_PATH = "/smsrio"
    INFISICAL_API_USERNAME = "API_USERNAME"
    INFISICAL_API_PASSWORD = "API_PASSWORD"
