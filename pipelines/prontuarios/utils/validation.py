from validate_docbr import CPF


def is_valid_cpf(cpf):
    """
    Validates a CPF number.

    Parameters
    ----------
    cpf: str
        CPF number to be validated.

    Returns
    -------
    bool
        True if CPF is valid, False otherwise.
    """
    if cpf is None:
        return False

    return CPF().validate(cpf)
