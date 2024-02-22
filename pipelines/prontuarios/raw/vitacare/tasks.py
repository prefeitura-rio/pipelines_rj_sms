# -*- coding: utf-8 -*-
"""
Tasks for Vitacare Raw Data Extraction
"""
from datetime import date, timedelta

import prefect
from prefect import task

from pipelines.prontuarios.raw.vitacare.constants import constants as vitacare_constants
from pipelines.prontuarios.utils.misc import group_data_by_cpf
from pipelines.utils.stored_variable import stored_variable_converter
from pipelines.utils.tasks import get_secret_key, load_from_api


@task(max_retries=3, retry_delay=timedelta(minutes=1))
@stored_variable_converter(output_mode="original")
def extract_data_from_api(
    cnes: str, target_day: date, entity_name: str, environment: str = "dev"
) -> dict:
    ap = vitacare_constants.CNES_TO_AP.value[cnes]
    api_url = vitacare_constants.AP_TO_API_URL.value[ap]
    endpoint = vitacare_constants.ENTITY_TO_ENDPOINT.value[entity_name]

    logger = prefect.context.get("logger")
    logger.info(f"Extracting data for {entity_name} on {target_day}")

    username = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=environment,
    )
    password = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=environment,
    )

    requested_data = load_from_api.run(
        url=f"{api_url}{endpoint}",
        params={
            "date": target_day,
            "cnes": cnes
        },
        auth_method="basic",
        credentials=(username, password)
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
        return group_data_by_cpf(data, lambda data: data["cpfPaciente"])
    elif entity_type == "pacientes":
        raise NotImplementedError("Entity pacientes not implemented yet.")
    else:
        raise ValueError(f"Invalid entity type: {entity_type}")
