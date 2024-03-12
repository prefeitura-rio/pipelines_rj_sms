# -*- coding: utf-8 -*-
import re
from typing import Callable

import dateutil.parser


def split_dataframe(df, chunk_size=10000):
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size : (i + 1) * chunk_size])  # noqa: E203
    return chunks


def build_additional_fields(
    data_list: list,
    cpf_get_function: Callable[[str], str],
    birth_data_get_function: Callable[[str], str],
    source_updated_at_get_function: Callable[[str], str],
) -> dict:
    """
    Builds additional fields for each data item in the given data list.

    Args:
        data_list (list): A list of data items.
        cpf_get_function (Callable[[str], str]): A function that takes a data item and
            returns the patient's CPF.
        birth_data_get_function (Callable[[str], str]): A function that takes a data
            item and returns the patient's birth date.
        source_updated_at_get_function (Callable[[str], str]): A function that takes a
            data item and returns the source updated date.

    Returns:
        dict: A dictionary containing additional fields for each data item.

    """

    items = []
    for data in data_list:
        # ------------------
        # Patient CPF and Patient Code
        # ------------------
        patient_cpf = cpf_get_function(data)
        if patient_cpf is None:
            patient_cpf = ""
        clean_patient_cpf = re.sub(r"\D", "", patient_cpf)

        birth_date = birth_data_get_function(data)
        birth_date = dateutil.parser.parse(birth_date)
        birth_date = birth_date.strftime("%Y%m%d")

        patient_code = f"{clean_patient_cpf}.{birth_date}"

        # ------------------
        # Source Updated At
        # ------------------
        source_updated_at = source_updated_at_get_function(data)
        source_updated_at = dateutil.parser.parse(source_updated_at)
        source_updated_at = source_updated_at.strftime("%Y-%m-%d %H:%M:%S")

        # ------------------
        # Joining
        # ------------------

        group = {
            "patient_cpf": clean_patient_cpf,
            "patient_code": patient_code,
            "source_updated_at": source_updated_at,
            "data": data,
        }
        items.append(group)

    return items
