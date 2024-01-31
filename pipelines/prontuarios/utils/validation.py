"""
Field validation functions for prontuario system.
"""

from validate_docbr import CPF
import pandas as pd


def is_valid_cpf(cpf):
    """
    Check if a CPF number is valid.

    Args:
        cpf (str): The CPF number to be validated.

    Returns:
        bool: True if the CPF is valid, False otherwise.
    """
    if cpf is None or pd.isna(cpf):
        return False

    return CPF().validate(cpf)
