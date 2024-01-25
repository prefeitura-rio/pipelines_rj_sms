# -*- coding: utf-8 -*-
# flake8: noqa: E203
from datetime import timedelta, date
import prefect
import requests

from prefect.client import Client
from prefect import task


from pipelines.prontuarios.utils.constants import constants as prontuario_constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.utils.validation import is_valid_cpf
from pipelines.utils.tasks import (
    get_secret_key
)

@task
def get_api_token(environment: str) -> str:
    """
    Get API Token

    Args:
        environment (str): Environment

    Returns:
        str: API Token
    """
    api_url=prontuario_constants.API_URL.value.get(environment)
    api_username=get_secret_key.run(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_API_USERNAME.value,
        environment=environment
    )
    api_password=get_secret_key.run(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_API_PASSWORD.value,
        environment=environment
    )
    response=requests.post(
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
def get_target_day() -> date:
    """
    Calculate the day the job is responsible for collecting data.

    Returns:
        date: Target job day
    """
    scheduled_start_time=prefect.context.get("scheduled_start_time")
    scheduled_date=scheduled_start_time.date() - timedelta(days=1)
    return scheduled_date


@task
def transform_to_raw_format(json_data:dict, cnes:str) -> dict:
    """
    Transform data to raw format

    Args:
        json_data (list[dict]): Data to transform
        cnes (list[str]): CNES list

    Returns:
        dict: Transformed data
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
    Load data to raw endpoint

    Args:
        dataframe (pd.DataFrame): Data to load
        endpoint_name (str): Endpoint name
        identifier_column (str, optional): Identifier column. Defaults to "patient_cpf".
    """    
    api_url=prontuario_constants.API_URL.value.get(environment)
    request_response=requests.post(
        url=f"{api_url}{endpoint_name}", 
        headers={'Authorization': f'Bearer {api_token}'},
        timeout=90,
        json=request_body
    )

    if request_response.status_code != 201:
        raise Exception(f"Error loading data to {endpoint_name} {request_response.json()}")
   

@task
def transform_create_input_batches(input_list: list, batch_size: int=250):
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
def rename_current_flow_run(environment: str, cnes:str) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_scheduled_time = prefect.context.get("scheduled_start_time").date()

    client = Client()
    client.set_flow_run_name(
        flow_run_id,
        f"(cnes={cnes}, env={environment}): {flow_run_scheduled_time}"
    )


@task
def transform_filter_valid_cpf(list_of_patients: list):
    paties_valid_cpf = filter(
        lambda patient: is_valid_cpf(patient['patient_cpf']),
        list_of_patients
    )
    return list(paties_valid_cpf)