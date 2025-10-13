# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Vitacare.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitacare flows
    """

    INFISICAL_PATH = "/prontuario-vitacare"
    INFISICAL_VITACARE_USERNAME = "USERNAME"
    INFISICAL_VITACARE_PASSWORD = "PASSWORD"

    ENDPOINT = {
        "posicao": "/reports/pharmacy/stocks",
        "movimento": "/reports/pharmacy/movements",
        "vacina": "/reports/vacinas/listagemvacina",
        "condicao": "/reports/attendances/attendanceparamcid",
    }

    DATASET_ID = "brutos_prontuario_vitacare"
