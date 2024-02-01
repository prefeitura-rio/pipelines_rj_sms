# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for Vitai Raw Data Extraction
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the dump vitai flows
    """

    INFISICAL_PATH = "/prontuario-vitai"
    INFISICAL_KEY = "TOKEN"
    INFISICAL_API_USERNAME = "API_USERNAME"
    INFISICAL_API_PASSWORD = "API_PASSWORD"

    API_URL = "http://api.token.hmrg.vitai.care/api/v1/"
