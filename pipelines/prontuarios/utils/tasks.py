# -*- coding: utf-8 -*-
# flake8: noqa: E203
"""
Utilities Tasks for prontuario system pipelines.
"""

from datetime import timedelta, date
import prefect
import requests
from prefect.client import Client
from prefect import task
from pipelines.prontuarios.constants import constants as prontuario_constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.utils.validation import is_valid_cpf
from pipelines.utils.tasks import (
    get_secret_key
)


@task
def get_api_token(
    environment: str,
    infisical_path: str,
    infisical_api_username: str,
    infisical_api_password: str
) -> str:
    """
    Retrieves the authentication token for Prontuario Integrado API.

    Args:
        environment (str): The environment for which to retrieve the API token.

    Returns:
        str: The API token.

    Raises:
        Exception: If there is an error getting the API token.
    """
    api_url = prontuario_constants.API_URL.value.get(environment)
    api_username = get_secret_key.run(
        secret_path=infisical_path,
        secret_name=infisical_api_username,
        environment=environment
    )
    api_password = get_secret_key.run(
        secret_path=infisical_path,
        secret_name=infisical_api_password,
        environment=environment
    )
    response = requests.post(
        url=f"{api_url}auth/token",
        timeout=90,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        data={
            'username': api_username,
            'password': api_password,
        },
    )

    if response.status_code == 200:
        return response.json()['access_token']
    else:
        raise Exception(f"Error getting API token ({response.status_code}) - {response.json()}")


@task
def get_flow_scheduled_day() -> date:
    """
    Returns the scheduled day for the flow execution.

    The scheduled day is calculated by subtracting one day from the scheduled start time.

    Returns:
        date: The scheduled day for the flow execution.
    """
    scheduled_start_time = prefect.context.get("scheduled_start_time")
    scheduled_date = scheduled_start_time.date() - timedelta(days=1)
    return scheduled_date


@task
def transform_to_raw_format(json_data: dict, cnes: str) -> dict:
    """
    Transforms the given JSON data to the accepted raw endpoint format.

    Args:
        json_data (dict): The JSON data to be transformed.
        cnes (str): The CNES value.

    Returns:
        dict: The transformed data in the accepted raw endpoint format.
    """
    return {
        "data_list": json_data,
        "cnes": cnes
    }


@task
def load_to_api(
    request_body: dict,
    endpoint_name: str,
    api_token: str,
    environment: str
) -> None:
    """
    Sends a POST request to the specified API endpoint with the provided request body.

    Args:
        request_body (dict): The JSON payload to be sent in the request.
        endpoint_name (str): The name of the API endpoint to send the request to.
        api_token (str): The API token used for authentication.
        environment (str): The environment to use for the API request.

    Raises:
        Exception: If the request fails with a status code other than 201.

    Returns:
        None
    """
    api_url = prontuario_constants.API_URL.value.get(environment)
    request_response = requests.post(
        url=f"{api_url}{endpoint_name}",
        headers={'Authorization': f'Bearer {api_token}'},
        timeout=90,
        json=request_body
    )

    if request_response.status_code != 201:
        raise Exception(f"Error loading data to {endpoint_name} {request_response.text}")


@task
def transform_create_input_batches(input_list: list, batch_size: int = 250):
    """
    Transform input list into batches

    Args:
        input_list (list): Input list
        batch_size (int, optional): Batch size. Defaults to 250.

    Returns:
        list[list]: List of batches
    """
    return [input_list[i:i+batch_size] for i in range(0, len(input_list), batch_size)]


@task
def rename_current_flow_run(environment: str, cnes: str, is_initial_extraction: bool) -> None:
    """
    Renames the current flow run using the specified environment and CNES.

    Args:
        environment (str): The environment of the flow run.
        cnes (str): The CNES (National Registry of Health Establishments) of the flow run.

    Returns:
        None
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_scheduled_time = prefect.context.get("scheduled_start_time").date()

    if is_initial_extraction:
        title = "Initialization"
    else:
        title = "Routine"

    client = Client()
    client.set_flow_run_name(
        flow_run_id,
        f"{title} (cnes={cnes}, env={environment}): {flow_run_scheduled_time}"
    )


@task
def transform_filter_valid_cpf(objects: list[dict]) -> list[dict]:
    """
    Filters the list of objects based on the validity of their CPF.

    Args:
        objects (list[dict]): A list of objects with 'patient_cpf' field.

    Returns:
        list[dict]: A list of objects that have valid CPFs.
    """
    obj_valid_cpf = filter(
        lambda obj: is_valid_cpf(obj['patient_cpf']),
        objects
    )
    return list(obj_valid_cpf)
