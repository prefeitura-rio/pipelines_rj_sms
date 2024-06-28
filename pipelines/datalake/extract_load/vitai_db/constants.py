# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0301
"""
Constants for Vitai DB Extraction.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the vitai db extraction flows
    """

    DATASET_NAME = "vitai_db"
    BQ_PROJECT_FROM_ENVIRONMENT = {
        "dev": "rj-sms-dev",
        "staging": "rj-sms-dev",
        "prod": "rj-sms",
        "local-prod": "rj-sms",
        "local-staging": "rj-sms-dev",
    }
