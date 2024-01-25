from datetime import date
from typing import Callable


def format_date_to_request(date_to_format: date):
    return date_to_format.strftime("%d/%m/%Y 00:00:00")


def group_data_by_cpf(
    data_list: list,
    cpf_get_function: Callable[[str], str]
):
    groups = []
    for data in data_list:
        group = {
            'patient_cpf': cpf_get_function(data),
            'data': data
        }
        groups.append(group)
    return groups
