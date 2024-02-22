# -*- coding: utf-8 -*-
from typing import Callable

import prefect


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size : (i + 1) * chunk_size])  # noqa: E203
    return chunks


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

        group = {"patient_cpf": patient_cpf, "data": data}
        groups.append(group)
        
    return groups