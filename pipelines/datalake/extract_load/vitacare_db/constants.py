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

    GCS_BUCKET = {
        "dev": "vitacare_db_dump_dev",
        "local-prod": "vitacare_db_dump",
        "prod": "vitacare_db_dump",
    }
    GDRIVE_FOLDER_ID = "1VUdm8fixnUs_dJrcflsNvzXIGPX6e-2r"
