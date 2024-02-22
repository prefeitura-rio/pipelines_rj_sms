# -*- coding: utf-8 -*-
"""
Utilities functions for Vitai Raw Data Extraction
"""
from datetime import date


def format_date_to_request(date_to_format: date) -> str:
    """
    Formats a date object to a string in the format "dd/mm/YYYY 00:00:00".

    Args:
        date_to_format (date): The date object to be formatted.

    Returns:
        str: The formatted date string.
    """
    return date_to_format.strftime("%d/%m/%Y 00:00:00")
