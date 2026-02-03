# -*- coding: utf-8 -*-
"""
Constants.
"""

from enum import Enum


class constants(Enum):
    """
    Constant values for the execute dbt flows
    """

    GCS_BUCKET = {
        "dev": "vitacare_informes_mensais_gdrive",
        "local-prod": "vitacare_informes_mensais_gdrive",
        "prod": "vitacare_informes_mensais_gdrive",
    }
