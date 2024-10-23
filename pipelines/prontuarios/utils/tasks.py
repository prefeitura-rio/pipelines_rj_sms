# -*- coding: utf-8 -*-
# flake8: noqa: E203
"""
Utilities Tasks for prontuario system pipelines.
"""

import gc
import hashlib
import json
from datetime import date, timedelta

import pandas as pd
import prefect
import requests
from prefect.client import Client

from pipelines.prontuarios.constants import constants as prontuario_constants
from pipelines.prontuarios.utils.misc import split_dataframe
from pipelines.prontuarios.utils.validation import is_valid_cpf
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.stored_variable import stored_variable_converter
from pipelines.utils.tasks import get_secret_key, load_file_from_bigquery


@task
def create_idempotency_keys(params: list[dict]):
    """
    Create a list of idempotency keys based on the given parameters.

    Args:
        params (list[dict]): A list of dictionaries with the parameters to be used to create the idempotency keys.

    Returns:
        list[str]: A list of idempotency keys.
    """
    keys = []
    for param in params:
        dump = json.dumps(param, sort_keys=True).encode()
        keys.append(hashlib.sha256(dump).hexdigest())

    return keys


@task
def get_current_flow_labels() -> list[str]:
    """
    Get the labels of the current flow.
    """
    from prefect.backend import FlowRunView

    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_view = FlowRunView.from_flow_run_id(flow_run_id)
    return flow_run_view.labels


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def get_api_token(
    environment: str,
    infisical_path: str,
    infisical_api_url: str,
    infisical_api_username: str,
    infisical_api_password: str,
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
    api_url = get_secret_key.run(
        secret_path="/", secret_name=infisical_api_url, environment=environment
    )
    api_username = get_secret_key.run(
        secret_path=infisical_path, secret_name=infisical_api_username, environment=environment
    )
    api_password = get_secret_key.run(
        secret_path=infisical_path, secret_name=infisical_api_password, environment=environment
    )
    response = requests.post(
        url=f"{api_url}auth/token",
        timeout=180,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "username": api_username,
            "password": api_password,
        },
    )

    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Error getting API token ({response.status_code}) - {response.json()}")


@task(nout=2)
def get_datetime_working_range(
    start_datetime: str = "", end_datetime: str = "", interval: int = 1, return_as_str: bool = False
):
    logger = prefect.context.get("logger")

    logger.info(f"Calculating datetime range...")
    if start_datetime == "" and end_datetime == "":
        logger.info(
            f"No start/end provided. Using {interval} as day interval and scheduled date as end"
        )  # noqa
        scheduled_datetime = prefect.context.get("scheduled_start_time")
        end_datetime = scheduled_datetime.date()
        start_datetime = end_datetime - timedelta(days=interval)
    elif start_datetime == "" and end_datetime != "":
        logger.info("No start provided. Using end datetime and provided interval")
        end_datetime = pd.to_datetime(end_datetime)
        start_datetime = end_datetime - timedelta(days=interval)
    elif start_datetime != "" and end_datetime == "":
        logger.info("No end provided. Using start datetime and provided interval")
        start_datetime = pd.to_datetime(start_datetime)
        end_datetime = start_datetime + timedelta(days=interval)
    else:
        logger.info("Start and end datetime provided. Using them.")
        start_datetime = pd.to_datetime(start_datetime)
        end_datetime = pd.to_datetime(end_datetime)

    logger.info(f"Target date range: {start_datetime} -> {end_datetime}")

    if return_as_str:
        return start_datetime.strftime("%Y-%m-%d %H:%M:%S"), end_datetime.strftime(
            "%Y-%m-%d %H:%M:%S"
        )

    return start_datetime, end_datetime


@task
@stored_variable_converter()
def transform_to_raw_format(json_data: dict, cnes: str) -> dict:
    """
    Transforms the given JSON data to the accepted raw endpoint format.

    Args:
        json_data (dict): The JSON data to be transformed.
        cnes (str): The CNES value.

    Returns:
        dict: The transformed data in the accepted raw endpoint format.
    """
    logger = prefect.context.get("logger")
    logger.info(f"Formatting {len(json_data)} registers")

    result = {"data_list": json_data, "cnes": cnes}

    return result


@task(max_retries=3, retry_delay=timedelta(minutes=3))
@stored_variable_converter()
def load_to_api(
    request_body: dict,
    endpoint_name: str,
    api_token: str,
    environment: str,
    api_url: str = None,
    method: str = "POST",
    parameters: dict = {},
    sucessfull_status_codes: list[int] = [200, 201],
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
    log(f"Loading data to API: {endpoint_name}")
    if api_url is None:
        api_url = get_secret_key.run(
            secret_path="/",
            secret_name=prontuario_constants.INFISICAL_API_URL.value,
            environment=environment,
        )
    request_response = requests.request(
        method=method,
        url=f"{api_url}{endpoint_name}",
        headers={"Authorization": f"Bearer {api_token}"},
        timeout=180,
        json=request_body,
        params=parameters,
    )

    if request_response.status_code not in sucessfull_status_codes:
        raise requests.exceptions.HTTPError(f"Error loading data: {request_response.text}")
    else:
        log(f"Data loaded successfully: [{request_response.status_code}] {request_response.text}")


@task
def rename_current_std_flow_run(environment: str, **kwargs) -> None:
    """
    Renames the current standardize flow run using the specified day

    Args:
        environment (str): The environment of the flow run

    Returns:
        None
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_scheduled_time = prefect.context.get("scheduled_start_time").date()

    title = "Standardization routine"

    params = [f"{key}={value}" for key, value in kwargs.items()]
    params.append(f"env={environment}")
    params = sorted(params)

    flow_run_name = f"{title} ({', '.join(params)}): {flow_run_scheduled_time}"

    client = Client()
    client.set_flow_run_name(flow_run_id, flow_run_name)


def rename_current_mrg_flow_run(environment: str, **kwargs) -> None:
    """
    Renames the current standardize flow run using the specified day

    Args:
        environment (str): The environment of the flow run

    Returns:
        None
    """
    flow_run_id = prefect.context.get("flow_run_id")
    flow_run_scheduled_time = prefect.context.get("scheduled_start_time").date()

    title = "Merge routine"

    params = [f"{key}={value}" for key, value in kwargs.items()]
    params.append(f"env={environment}")
    params = sorted(params)

    flow_run_name = f"{title} ({', '.join(params)}): {flow_run_scheduled_time}"

    client = Client()
    client.set_flow_run_name(flow_run_id, flow_run_name)


@task
@stored_variable_converter()
def transform_filter_valid_cpf(objects: list[dict]) -> list[dict]:
    """
    Filters the list of objects based on the validity of their CPF.

    Args:
        objects (list[dict]): A list of objects with 'patient_cpf' field.

    Returns:
        list[dict]: A list of objects that have valid CPFs.
    """

    obj_valid_cpf = filter(lambda obj: is_valid_cpf(obj["patient_cpf"]), objects)
    obj_valid_cpf = list(obj_valid_cpf)

    logger = prefect.context.get("logger")
    logger.info(f"Filtered {len(objects)} registers to {len(obj_valid_cpf)} valid CPFs")

    return obj_valid_cpf


@task
def transform_split_dataframe(
    dataframe: pd.DataFrame, batch_size: int = 1000
) -> list[pd.DataFrame]:

    return split_dataframe(dataframe, chunk_size=batch_size)


@task
@stored_variable_converter()
def transform_create_input_batches(input_list: list, batch_size: int = 250):
    """
    Transform input list into batches

    Args:
        input_list (list): Input list
        batch_size (int, optional): Batch size. Defaults to 250.

    Returns:
        list[list]: List of batches
    """
    result = [input_list[i : i + batch_size] for i in range(0, len(input_list), batch_size)]

    return result


@task
def force_garbage_collector():
    """
    Forces garbage collector to run.

    Returns:
        None
    """

    gc.collect()
    return None


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


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def get_ap_from_cnes(cnes: str) -> str:

    dados_mestres = load_file_from_bigquery.run(
        project_name="rj-sms", dataset_name="saude_dados_mestres", table_name="estabelecimento"
    )

    unidade = dados_mestres[dados_mestres["id_cnes"] == cnes]

    if unidade.empty:
        raise KeyError(f"CNES {cnes} not found in the database")

    ap = unidade.iloc[0]["area_programatica"]

    return f"AP{ap}"


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def get_healthcenter_name_from_cnes(cnes: str) -> str:

    dados_mestres = load_file_from_bigquery.run(
        project_name="rj-sms", dataset_name="saude_dados_mestres", table_name="estabelecimento"
    )

    unidade = dados_mestres[dados_mestres["id_cnes"] == cnes]

    if unidade.empty:
        raise KeyError(f"CNES {cnes} not found in the database")

    nome_limpo = unidade.iloc[0]["nome_limpo"]
    ap = unidade.iloc[0]["area_programatica"]

    return f"(AP{ap}) {nome_limpo}"


@task
def get_project_name(environment: str):
    """
    Returns the project name based on the given environment.

    Args:
        environment (str): The environment for which to retrieve the project name.

    Returns:
        str: The project name corresponding to the given environment.
    """
    return constants.PROJECT_NAME.value[environment]


@task
def get_project_name_from_flow_environment():
    log(f"Prefect context: {prefect.context}")
    return prefect.context.get("project_name")

