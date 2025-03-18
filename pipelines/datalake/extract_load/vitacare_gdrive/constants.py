"""
Constants for DBT.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the execute dbt flows
    """

    GCS_BUCKET = {
        "dev": "vitacare_reports_gdrive_dev",
        "local-prod": "vitacare_reports_gdrive",
        "prod": "vitacare_reports_gdrive",
    }
    GDRIVE_FOLDER_ID = "1H_49fLhbT0bWYk8gBKLOdYHgT_xODpg7"
