# -*- coding: utf-8 -*-
import re

import prefect
from prefect import task


@task
def standardize_race(patient_list):
    """
    Standardizes the race information in the given data dictionary.

    Args:
        data (dict): The data dictionary containing the race information.

    Returns:
        dict: The updated data dictionary with the race information standardized.
    """
    for patient in patient_list:
        data = patient.get("data")

        if data.get("racaCor") is None:
            continue
        elif bool(re.search("SEM INFO", data.get("racaCor"))):
            continue
        else:
            data["race"] = data.get("racaCor").lower()
            continue

    return patient_list
