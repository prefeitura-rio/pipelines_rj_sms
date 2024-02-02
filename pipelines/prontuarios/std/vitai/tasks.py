# -*- coding: utf-8 -*-
# flake8: noqa: E203
"""
Utilities Tasks for prontuario system pipelines.
"""

import requests
import json
from prefect import task
from pipelines.prontuarios.constants import constants as prontuario_constants
from pipelines.prontuarios.std.vitai.constants import constants as smsrio_constants
import pandas as pd
from pipelines.utils.tasks import (
    get_secret_key
)
from pipelines.prontuarios.std.utils import (
    gender_validation,
    nationality_validation,
    city_cod_validation,
    state_cod_validation
)


@task
def get_database_url(environment):
    """
    Get SMSRio database url from Infisical Secrets

    Args:
        environment (str): Environment

    Returns:
        str: Database url
    """
    database_url = get_secret_key.run(
        secret_path=smsrio_constants.INFISICAL_PATH.value,
        secret_name=smsrio_constants.INFISICAL_DB_URL.value,
        environment=environment
    )
    return database_url


@task
def load_from_api(
    params: dict,
    endpoint_name: str,
    api_token: str,
    environment: str
) -> None:
    """
    Sends a GET request to the specified API endpoint with the provided request body.

    Args:
        params (dict): Parameters used for calling the API
        endpoint_name (str): The name of the API endpoint to send the request to.
        api_token (str): The API token used for authentication.
        environment (str): The environment to use for the API request.

    Raises:
        Exception: If the request fails with a status code other than 201.

    Returns:
        json: API response
    """
    api_url = prontuario_constants.API_URL.value.get(environment)

    args = ['{}={}'.format(k, v) for k, v in params.items()]
    args_in_one_string = "&".join(args)
    url = f"{api_url}{endpoint_name}" + "?" + args_in_one_string

    request_response = requests.get(
        url=url,
        headers={'Authorization': f'Bearer {api_token}'},
        timeout=90
    )

    if request_response.status_code != 200:
        raise Exception(f"Error getting data from {endpoint_name} {request_response.json()}")

    return request_response.text


@task
def standartize_data(
    data: dict,
) -> dict:
    """
    Sends a GET request to the specified API endpoint with the provided request body.

    Args:
        data (dict): Raw data from API

    Returns:
        dict: Standartized data ready to load to API
    """
    data = json.loads(data)
    std_data = []

    for patient in data:

        patient_std = {

            "active": True,
            "birth_city_cod": city_cod_validation(patient['data']['municipio']),  # mudar codigo
            "birth_state_cod": state_cod_validation(patient['data']['uf']),  # mudar codigo
            "birth_country_cod": "1",
            "birth_date": str(pd.to_datetime(patient['data']['dataNascimento'], format='%Y-%m-%d %H:%M:%S').date()) if patient['data']['dataNascimento'] != None else "0",
            "patient_cpf": patient['patient_cpf'] if patient['patient_cpf'] is not None else "0",
            "deceased": False if patient['data']['dataObito'] is None else True,
            "deceased_date": str(pd.to_datetime(patient['data']['dataObito'], format='%Y-%m-%d %H:%M:%S').date()) if patient['data']['dataObito'] != None else None,
            "father_name": patient['data']['nomePai'].title() if patient['data']['nomePai'] is not None else None,
            "gender": gender_validation(patient['data']['sexo']),
            "mother_name": patient['data']['nomeMae'].title() if patient['data']['nomeMae'] is not None else None,
            "name": patient['data']['nome'].title() if patient['data']['nome'] is not None else "0",
            "nationality": nationality_validation(patient['data']['nacionalidade']),
            "protected_person": True,
            "race": patient['data']['racaCor'].lower() if patient['data']['racaCor'] is not None else None,
            "cns_list": [
                # adaptar para varios cns
                {
                    "value": patient['data']['cns'] if patient['data']['cns'] is not None else None,
                    "is_main": True
                }
            ],
            "address_list": [
                {  # adaptar para varios end
                    "use": None,
                    "type": None,
                    "line": patient['data']['tipoLogradouro'].title() + ' ' + patient['data']['nomeLogradouro'].title() if ((patient['data']['tipoLogradouro'] != None) & (patient['data']['nomeLogradouro'] != None)) else "0",
                    "city": city_cod_validation(patient['data']['municipio']),
                    "country": "1",  # mudar codigo
                    "state": state_cod_validation(patient['data']['uf']),
                    "postal_code": "0",
                    "start": None,
                    "end": None
                }
            ],
            "telecom_list": [{
                "system": None,
                "use": None,
                "value": patient['data']['telefone'] if patient['data']['telefone'] is not None else "0",
                "rank": 0,
                "start": None,
                "end": None
            }
            ],
            "raw_source_id": patient['id']  # entender melhor pq temos dois id e qual utilizar
        }
        std_data.append(patient_std)

    std_data = std_data[0:2]  # retirar dps

    return std_data


@task
def load_to_api(
    request_body: dict,
    endpoint_name: str,
    api_token: str,
    environment: str
) -> None:
    """
    Sends a GET request to the specified API endpoint with the provided request body.

    Args:
        params (dict): Parameters used for calling the API
        endpoint_name (str): The name of the API endpoint to send the request to.
        api_token (str): The API token used for authentication.
        environment (str): The environment to use for the API request.

    Raises:
        Exception: If the request fails with a status code other than 201.

    Returns:
        json: API response
    """
    api_url = prontuario_constants.API_URL.value.get(environment)
    url = f"{api_url}{endpoint_name}"
    request_response = requests.post(
        url=url,
        headers={'Authorization': f'Bearer {api_token}'},
        timeout=90,
        json=request_body
    )

    if request_response.status_code != 201:
        raise Exception(
            f"Error getting data from {endpoint_name} {request_response.status_code} {request_response.json()}")

    return request_response.text
