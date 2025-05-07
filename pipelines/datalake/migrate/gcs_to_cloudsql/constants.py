# -*- coding: utf-8 -*-
"""
Constants.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the execute dbt flows
    """

    PROJECT_ID = "rj-sms-dev"
    SERVICE_ACCOUNT_FILE = "/tmp/credentials.json"
    CSQL_URL = "https://sqladmin.googleapis.com"
    CSQL_PATH = "/sql/v1beta4/projects/{project}/instances/{instance}/import"
