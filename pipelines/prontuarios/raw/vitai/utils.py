# -*- coding: utf-8 -*-
"""
Utilities functions for Vitai Raw Data Extraction
"""
import prefect
from datetime import date
from typing import Callable


def format_date_to_request(date_to_format: date) -> str:
    """
    Formats a date object to a string in the format "dd/mm/YYYY 00:00:00".

    Args:
        date_to_format (date): The date object to be formatted.

    Returns:
        str: The formatted date string.
    """
    return date_to_format.strftime("%d/%m/%Y 00:00:00")


def group_data_by_cpf(data_list: list, cpf_get_function: Callable[[str], str]) -> dict:
    """
    Groups the data in the given list by patient CPF using the provided CPF get function.

    Args:
        data_list (list): The list of data to be grouped.
        cpf_get_function (Callable[[str], str]): A function that takes a data item and
                                                 returns the patient CPF.

    Returns:
        dict: A dictionary containing the grouped data, where each key is the patient CPF
              and the value is the corresponding data item.
    """
    logger = prefect.context.get("logger")

    groups = []
    for data in data_list:
        try:
            patient_cpf = cpf_get_function(data)
        except TypeError as e:
            logger.warning(f"Skipping data item: {data}. Reason: {e}")
            continue

        group = {
            "patient_cpf": patient_cpf, 
            "data": data
        }
        groups.append(group)

    return groups
