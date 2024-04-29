# -*- coding: utf-8 -*-
from datetime import timedelta

import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.prontuarios.mrg.functions import (
    final_merge,
    first_merge,
    load_ranking,
    normalize_payload_list,
    sanity_check,
)
from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_params(start_datetime: str, end_datetime: str) -> dict:
    """
    Creating params
    Args:
        start_datetime (str) : initial date extraction

    Returns:
        dict : params dictionary
    """
    # start_datetime = '2024-03-14 17:03:25'
    # end_datetime = '2024-03-14 17:03:26' #start_datetime + timedelta(days=1)
    log(
        f"""
        Standardizing from {start_datetime}
        to {end_datetime}"""
    )
    return {
        "start_datetime": start_datetime,  # .strftime("%Y-%m-%d 00:00:00"),
        "end_datetime": end_datetime,  # .strftime("%Y-%m-%d 00:00:00")
    }


@task
def print_n_patients(data: list):
    """
    Print number of patients to perform merge

    Args:
        data (list): List of patients to perform merge.
    """

    log(f"Merging registers for {len(data)} patients")

    return data


@task
def load_mergeable_data(url: str, cpfs: list, credentials: str) -> list:
    """
    Loads mergeable data of patient from std patient API endpoint.

    Args:
        url (str): The URL of the API endpoint.
        credentials (str): The authentication credentials.

    Returns:
        dict: The JSON response from the API.

    Raises:
        Exception: If the API request fails.
    """
    headers = {"Authorization": f"Bearer {credentials}"}
    data = []
    for cpf in cpfs:
        response = requests.get(url + "/" + cpf, headers=headers, timeout=180)
        if response.status_code == 200:
            data.append(response.json())
        else:
            raise ValueError(f"API call failed, error: {response.status_code} - {response.reason}")
    return data


@task
def merge(data_to_merge) -> dict:
    """
    Loads mergeable data of patient from std patient API endpoint.

    Args:
        data_to_merge (list): Standardized data registers related to patient_code ready to merge

    Returns:
        sanitized_data (list): Merged data
    """
    register_list = list(map(normalize_payload_list, data_to_merge))
    log("Trying first merge")
    ranking_df = load_ranking()
    merged_n_not_merged_data = list(map(lambda x: first_merge(x, ranking_df), register_list))
    log("Final merge")
    merged_data = list(map(final_merge, merged_n_not_merged_data))
    log("Sanity check")
    sanitized_data = list(map(sanity_check, merged_data))
    return sanitized_data


@task(max_retries=3, retry_delay=timedelta(minutes=3))
def put_to_api(request_body: dict, api_url: str, endpoint_name: str, api_token: str) -> None:
    """
    Sends a PUT request to the specified API endpoint with the provided request body.

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
    request_response = requests.put(
        url=f"{api_url}{endpoint_name}",
        headers={"Authorization": f"Bearer {api_token}"},
        timeout=180,
        json=request_body,
    )

    if request_response.status_code not in [200, 201]:
        raise requests.exceptions.HTTPError(f"Error loading data: {request_response.text}")
