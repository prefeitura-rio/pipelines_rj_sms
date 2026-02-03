# -*- coding: utf-8 -*-
"""
Constants.
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the execute dbt flows
    """

    SERVICE_ACCOUNT_FILE = "/tmp/credentials.json"

    PROJECT_ID = "rj-sms-dev"
    API_BASE = "https://sqladmin.googleapis.com/sql/v1beta4/projects/rj-sms-dev"  # PROJECT_ID
