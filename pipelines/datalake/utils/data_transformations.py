# -*- coding: utf-8 -*-
# pylint: disable= C0301
"""
Data transformations functions
"""

from datetime import date, timedelta


def convert_str_to_date(target_date: str) -> str:
    """
    Convert a string to a date in the format YYYY-MM-DD

    :param target_date: The target date to convert
    :type target_date: str
    :return: The target date in the format YYYY-MM-DD
    :rtype: str
    """
    if target_date == "today":
        return date.today().strftime("%Y-%m-%d")
    elif target_date == "yesterday":
        return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        try:
            return target_date
        except ValueError as e:
            raise ValueError("The target_date must be in the format YYYY-MM-DD") from e
