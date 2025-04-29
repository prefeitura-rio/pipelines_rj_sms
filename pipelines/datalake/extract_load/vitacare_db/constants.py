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
        "dev": "vitacare_backups_gdrive",
        "local-prod": "vitacare_backups_gdrive",
        "prod": "vitacare_backups_gdrive",
    }
    GDRIVE_FOLDER_ID = "1VUdm8fixnUs_dJrcflsNvzXIGPX6e-2r"
    BACKUP_SUBFOLDER = "HISTÓRICO_PEPVITA_RJ"
