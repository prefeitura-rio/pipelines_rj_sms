# -*- coding: utf-8 -*-

from enum import Enum


class rnds_constants(Enum):
    INFISICAL_PATH = "/prontuario-vitacare-database"
    INFISICAL_PORT = "DATABASE_PORT"
    INFISICAL_HOST = "DATABASE_HOST"
    INFISICAL_USERNAME = "DATABASE_USER"
    INFISICAL_PASSWORD = "DATABASE_PASSWORD"
    DATASET_ID = "brutos_rnds_historico"
    DB_NAME = "rnds_historic"
    DB_SCHEMA = "dbo"
    BQ_PARTITION_COLUMN = "DATA_ADMINISTRACAO"
    AP_LIST = [
        "AP10",
        "AP21",
        "AP22",
        "AP31",
        "AP32",
        "AP33",
        "AP40",
        "AP51",
        "AP52",
        "AP53",
    ]
    TABLE_SUFFIX = "RNDS_RIA_ROUTINE_IMMUNIZATION_CONTROL"