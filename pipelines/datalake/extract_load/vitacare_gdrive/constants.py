# -*- coding: utf-8 -*-
"""
Constants for DBT.
"""
from enum import Enum


class constants(Enum):
    """
    Constant values for the execute dbt flows
    """

    GCS_BUCKET = {
        "dev": "vitacare_informes_mensais",
        "local-prod": "vitacare_informes_mensais",
        "prod": "vitacare_informes_mensais",
    }

    GDRIVE_FOLDER_STRUCTURE = {
        "PRISIONAL": "1MQccwkVqtvaHP2kwRux8WSQguB-i1X_8",
        "AP53": "1nPz_-2HUagcDQdiU3hHmBplZRw7VV6UW",
        "AP52": "1CBXGrJzfYbf-c1THu0XzI1AQ5rkdMv7Y",
        "AP51": "1e3ytTkLXDw9uvlf_-G0s9KEddSiqao17",
        "AP40": "139VrWwOxUQjcJExSbyAlVuuMWRd7J0U6",
        "AP33": "1UPtqhBQ2eI6IsWIF_4N5i1hmIlfvtT_q",
        "AP32": "1Phv5U26uCaVaK1uk7rrYwFYxErw74ScM",
        "AP31": "1EGJsUj2qCkTRRRBuNKfJOZCwTOpfXk4H",
        "AP22": "1CNkTJ4hwhCUuaoH7p5xFRN3adp6MNCys",
        "AP21": "1YkdBB9lxNQQUuiUWF-71GqhFXLwEdQp0",
        "AP10": "1aFZPOfB-3XXUSEVZLQHrb0--V6cJTkRu",
    }
