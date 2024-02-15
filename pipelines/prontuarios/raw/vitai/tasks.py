# -*- coding: utf-8 -*-
"""
Tasks for Vitai Raw Data Extraction
"""
from datetime import date, timedelta

import prefect
from prefect import task

from pipelines.prontuarios.raw.vitai.constants import constants as vitai_constants
from pipelines.prontuarios.raw.vitai.utils import (
    format_date_to_request,
    group_data_by_cpf,
)
from pipelines.utils.tasks import get_secret_key, load_from_api
from pipelines.utils.stored_variable import stored_variable_converter


@task
def get_vitai_api_token(environment: str = "dev") -> str:
    """
    Retrieves the API token for the Vitai API.

    Args:
        environment (str, optional): The environment to retrieve the token for. Defaults to 'dev'.

    Returns:
        str: The API token.
    """
    token = get_secret_key.run(
        secret_path=vitai_constants.INFISICAL_PATH.value,
        secret_name=vitai_constants.INFISICAL_KEY.value,
        environment=environment,
    )
    return token


@task
@stored_variable_converter(output_mode='transform')
def extract_data_from_api(
    url: str, target_day: date, entity_name: str, vitai_api_token: str
) -> dict:
    """
    Extracts data from the Vitai API for a specific target day and entity name.

    Args:
        target_day (date): The target day for which data needs to be extracted.
        entity_name (str): The name of the entity for which data needs to be extracted.
                           Valid values are 'pacientes' and 'diagnostico'.
        vitai_api_token (str): The API token for accessing the Vitai API.

    Returns:
        dict: The extracted data from the Vitai API.
    """
    assert entity_name in ["pacientes", "diagnostico"], f"Invalid entity name: {entity_name}"

    request_url = url + f"{entity_name}/listByPeriodo"

    logger = prefect.context.get("logger")
    logger.info(f"Extracting data for {entity_name} on {target_day}")

    requested_data = load_from_api.run(
        url=request_url,
        params={
            "dataInicial": format_date_to_request(target_day),
            "dataFinal": format_date_to_request(target_day + timedelta(days=1)),
        },
        credentials=vitai_api_token,
    )

    return requested_data


@task
@stored_variable_converter()
def group_data_by_patient(data: list[dict], entity_type: str) -> dict:
    """
    Groups the data list by patient CPF.

    Args:
        data (list): The list of dict data.
        entity (str): The entity name. This is used to choose the path to the
            patient cpf.

    Returns:
        dict: A dictionary where the keys are patient identifiers and the values are
            lists of entity data.

    Raises:
        ValueError: If the entity name is invalid.
    """
    if entity_type == "diagnostico":
        return group_data_by_cpf(data, lambda data: data["boletim"]["paciente"]["cpf"])
    elif entity_type == "pacientes":
        return group_data_by_cpf(data, lambda data: data["cpf"])
    else:
        raise ValueError(f"Invalid entity type: {entity_type}")


@task
def get_entity_endpoint_name(entity: str) -> str:
    """
    Returns the endpoint for the entity.

    Args:
        entity (str): The entity name.

    Returns:
        str: The endpoint for the entity.
    """
    if entity == "pacientes":
        return "raw/patientrecords"
    elif entity == "diagnostico":
        return "raw/patientconditions"
    else:
        raise ValueError(f"Invalid entity name: {entity}")


@task
def get_dates_in_range(minimum_date: date | str, maximum_date: date | str) -> list[date]:
    """
    Returns a list of dates from the minimum date to the current date.

    Args:
        minimum_date (date): The minimum date.

    Returns:
        list: The list of dates.
    """
    if isinstance(maximum_date, str):
        maximum_date = date.fromisoformat(maximum_date)

    if minimum_date == "":
        return [maximum_date]

    if isinstance(minimum_date, str):
        minimum_date = date.fromisoformat(minimum_date)

    return [minimum_date + timedelta(days=i) for i in range((maximum_date - minimum_date).days)]
