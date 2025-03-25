# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for SMSRio Raw Data Extraction
"""
from enum import Enum


class constants(Enum):

    INFISICAL_PATH = "/smsrio"
    INFISICAL_DB_URL = "DB_URL"
    INFISICAL_API_USERNAME = "API_USERNAME"
    INFISICAL_API_PASSWORD = "API_PASSWORD"

    SMSRIO_CNES = "7106513"
    SMSRIO_BUCKET = "prontuario-integrado"
    SMSRIO_FILE_NAME = "smsrio_dump_2024-04/smsrio_paciente_cns_telefone.csv"

    DATASET_ID = "brutos_plataforma_smsrio"
    TABLE_ID = "cadastro_paciente"