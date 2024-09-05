# -*- coding: utf-8 -*-
"""
Data transformation functions to vitalcare API data
"""


def classify_access(properties: dict):
    """
    Classifies the access level based on the dbt's manifest properties dictionary.

    Args:
        properties (dict): A dictionary containing the properties.

    Returns:
        str: The access level classification.

    Raises:
        None
    """
    try:
        labels = properties["config"]["labels"]
    except KeyError:
        return "nao_definido"

    keys = ["dado_publico", "dado_sensivel_saude", "dado_pessoal", "dado_anonimizado"]

    if all(key in labels.keys() for key in keys):
        if labels["dado_pessoal"] == "sim":
            if labels["dado_publico"] == "sim":
                return "publico"
            else:
                if labels["dado_sensivel_saude"] == "nao" and labels["dado_anonimizado"] == "sim":
                    return "interno"
                elif (
                    labels["dado_sensivel_saude"] == "sim" and labels["dado_anonimizado"] == "sim"
                ) or (
                    labels["dado_sensivel_saude"] == "nao" and labels["dado_anonimizado"] == "nao"
                ):
                    return "confidencial"
                elif labels["dado_sensivel_saude"] == "sim" and labels["dado_anonimizado"] == "nao":
                    return "restrito"
                else:
                    return "nao_definido"
        else:
            if labels["dado_publico"] == "sim":
                return "publico"
            else:
                return "interno"
    else:
        return "nao_definido"
