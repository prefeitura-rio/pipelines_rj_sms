# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for "Prontuário Integrado" system.
"""
from enum import Enum


class constants(Enum):

    API_URL = {
        "dev": "https://prontuario-integrado-staging-qchcir6qlq-uc.a.run.app/",
        "prod": "https://prontuario-integrado-production-qchcir6qlq-uc.a.run.app/",
    }
