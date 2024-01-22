# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for "Prontuário Integrado" system.
"""
from enum import Enum


class constants(Enum):

    API_URL = {
        "dev": "https://unificacao-prontuarios-dev.dados.rio/",
        "prod": "https://unificacao-prontuarios.dados.rio/",
    }
