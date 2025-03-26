# -*- coding: utf-8 -*-
"""
Field validation functions for prontuario system.
"""

import pandas as pd
from validate_docbr import CPF


def is_valid_cpf(cpf):
    """
    Check if a CPF number is valid.

    Args:
        cpf (str): The CPF number to be validated.

    Returns:
        bool: True if the CPF is valid, False otherwise.
    """
    if cpf is None or pd.isna(cpf) or cpf == "01234567890":
        return False

    return CPF().validate(cpf)
