# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for SMSRio.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump hci flow
    """

    INFISICAL_PATH = "/"
    INFISICAL_DB_URL = "PRONTUARIOS_DB_URL"
    SCHEMA_ID = ""
    DATASET_ID = "historico_clinico_integrado"
    TABLE_ID = {"mrg__patient": "paciente",
                "mrg__patientcns":"paciente_cns",
                "mrg__patientaddress":"paciente_endereco",
                "mrg__patienttelecom":"paciente_contato"}
