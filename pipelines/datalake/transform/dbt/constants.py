# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Constants for DBT.
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the execute dbt flows
    """

    REPOSITORY_URL = "https://github.com/prefeitura-rio/queries-rj-sms.git"
    GCS_BUCKET = {"prod": "rj-sms_dbt", "dev": "rj-sms-dev_dbt"}
