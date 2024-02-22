# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Vitacare Raw Data Extraction
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    INFISICAL_PATH = "/prontuario-vitacare"

    INFISICAL_VITACARE_USERNAME = "USERNAME"
    INFISICAL_VITACARE_PASSWORD = "PASSWORD"

    INFISICAL_API_USERNAME = "API_USERNAME"
    INFISICAL_API_PASSWORD = "API_PASSWORD"

    AP_TO_API_URL = {
        "AP10": "http://consolidado-ap10.pepvitacare.com:8088/",
    }
    CNES_TO_AP = {
        "5621801": "AP10",
    }

    ENTITY_TO_ENDPOINT = {
        'diagnostico': 'reports/attendances/attendanceparamcid',
        'pacientes': 'reports/patients/new_or_update'
    }
