# -*- coding: utf-8 -*-
"""
Tasks for Vitacare Raw Data Extraction
"""
import json
from datetime import date, timedelta

import prefect
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Failed

from pipelines.prontuarios.raw.vitacare.constants import constants as vitacare_constants
from pipelines.prontuarios.utils.misc import group_data_by_cpf
from pipelines.utils.stored_variable import stored_variable_converter
from pipelines.utils.tasks import (
    cloud_function_request,
    get_secret_key,
    load_file_from_bigquery,
)


@task(max_retries=4, retry_delay=timedelta(minutes=15))
@stored_variable_converter(output_mode="original")
def extract_data_from_api(
    cnes: str, ap: str, target_day: date, entity_name: str, environment: str = "dev"
) -> dict:
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

    response = cloud_function_request.run(
        url=f"{api_url}{endpoint}",
        request_type="GET",
        query_params={"date": str(target_day), "cnes": cnes},
        credential={"username": username, "password": password},
        env=environment,
    )

    if response["status_code"] != 200:
        raise ValueError(f"Failed to extract data from API: {response['status_code']}")

    requested_data = json.loads(response["body"])

    if len(requested_data) > 0:
        logger.info(f"Successful Request: retrieved {len(requested_data)} registers")
    else:
        logger.error("Failed Request: no data was retrieved")
        raise ENDRUN(
            state=Failed(f"Empty response for ({cnes}, {target_day}, {entity_name})")
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


@task
def create_parameter_list(environment: str = "dev"):
    """
    Create a list of parameters for running the Vitacare flow.

    Args:
        environment (str, optional): The environment to run the flow in. Defaults to "dev".

    Returns:
        list: A list of dictionaries containing the flow run parameters.
    """
    # Access the health units information from BigQuery table
    dados_mestres = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name="saude_dados_mestres",
        table_name="estabelecimento",
    )
    # Filter the units using Vitacare
    unidades_vitacare = dados_mestres[dados_mestres["prontuario_versao"] == "vitacare"]

    # Get their CNES list
    cnes_vitacare = unidades_vitacare["id_cnes"].tolist()

    # Construct the parameters for the flow
    vitacare_flow_parameters = []
    for entity in ["diagnostico"]:
        for cnes in cnes_vitacare:
            vitacare_flow_parameters.append(
                {
                    "cnes": cnes,
                    "entity": entity,
                    "environment": environment,
                    "rename_flow": True,
                }
            )

    logger = prefect.context.get("logger")
    logger.info(f"Created {len(vitacare_flow_parameters)} flow run parameters")

    return vitacare_flow_parameters
