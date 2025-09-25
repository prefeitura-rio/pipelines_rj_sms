# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0301
"""
Constants for biomega.
"""
from enum import Enum


class biomega_constants(Enum):
    """
    Constant values for the dump biomega flows
    """

    INFISICAL_PATH = "/biomega"
    INFISICAL_USERNAME = "USERNAME"
    INFISICAL_PASSWORD = "PASSWORD"
    INFISICAL_APCCODIGO = "APCCODIGO"
    INFISICAL_CODIGOLIS = "MAPEAMENTO_CNES_LIS"
    INFISICAL_AP_LIS = "MAPEAMENTO_AP_LIS"
    DATASET_ID = "brutos_biomega"