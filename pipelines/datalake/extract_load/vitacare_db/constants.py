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

    GCS_BUCKET = {"dev": "vitacare_db_dump_dev", "prod": "vitacare_db_dump"}
