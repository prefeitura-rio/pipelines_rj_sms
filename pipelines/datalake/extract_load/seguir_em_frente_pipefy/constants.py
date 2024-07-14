# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Pipefy.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for pipefy flows.
    """
    
    INFISICAL_PATH = "/seguir-em-frente"
    INFISICAL_PIPEFY_CLIENT_ID = "PIPEFY_CLIENT_ID"
    INFISICAL_PIPEFY_CLIENT_SECRET = "PIPEFY_CLIENT_SECRET"

    ENPOINT = {
        "bolsista": {
            "pipe_name": "Controle de Bolsista",
            "pipe_id": "304238257",
            "report_id": "300669503",
        },
        "presenca": {
            "pipe_name": "Controle de Presença",
            "pipe_id": "304104198",
            "report_id": "300712294",
        },
    }
