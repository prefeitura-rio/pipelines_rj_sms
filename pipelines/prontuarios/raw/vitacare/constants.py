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
        "AP21": "http://consolidado-ap21.pepvitacare.com:8090/",
        "AP22": "http://consolidado-ap22.pepvitacare.com:8091/",
        "AP31": "http://consolidado-ap31.pepvitacare.com:8089/",
        "AP32": "http://consolidado-ap32.pepvitacare.com:8090/",
        "AP33": "http://consolidado-ap33.pepvitacare.com:8089/",
        "AP40": "http://consolidado-ap40.pepvitacare.com:8089/",
        "AP51": "http://consolidado-ap51.pepvitacare.com:8091/",
        "AP52": "http://consolidado-ap52.pepvitacare.com:8088/",
        "AP53": "http://consolidado-ap53.pepvitacare.com:8092/",
    }

    ENTITY_TO_ENDPOINT = {
        "diagnostico": "reports/attendances/attendanceparamcid",
        "pacientes": "reports/patients/new_or_update",
    }
