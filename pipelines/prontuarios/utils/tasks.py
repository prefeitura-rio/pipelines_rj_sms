import json
from validate_docbr import CPF

from datetime import timedelta, date
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.prontuarios.utils.constants import constants as prontuario_constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.utils.tasks import (
    get_secret_key
)
import prefect
import pandas as pd
import requests

from datetime import date, timedelta


@task
def transform_create_input_batches(input_list, batch_size=250):
    """
    Create input batches

    Args:
        input (list): Input data
        batch_size (int, optional): Batch size. Defaults to 250.

    Returns:
        list: Input batches
    """
    return [input_list[i:i + batch_size] for i in range(0, len(input_list), batch_size)]


@task
def extract_tabledata_from_db(
    db_url              : str,
    tablename           : str,
    min_date            : str = None,
    max_date            : str = None,
    columns_list        : list[str] | str = '*',
    date_lookup_field   : str = 'updated_at'
) -> pd.DataFrame:
    """
    Extract data from a table from a given date

    Args:
        db_url (str): Database url (as in sqlalchemy)
        tablename (str): Table name
        min_date (str, optional): Minimum date to extract. Defaults to None.
        max_date (str, optional): Maximum date to extract. Defaults to None.
        columns_list (list[str], optional): List of columns to extract. Defaults to ['*'].
        date_lookup_field (str, optional): Date field to filter. Defaults to 'updated_at'.

    Returns:
        pd.DataFrame: Dataframe with the extracted data
    """
    assert isinstance(columns_list, list) or columns_list == '*', "columns_list must be a list or '*'"

    query = f"SELECT {', '.join(columns_list)} FROM {tablename}"

    if min_date or max_date:
        filter_parts = []
        if min_date:
            filter_parts.append( f"{date_lookup_field} >= '{min_date}'" )
        if max_date:
            filter_parts.append( f"{date_lookup_field} < '{max_date}'" )
        query += " WHERE " + " AND ".join(filter_parts)

    result = pd.read_sql(query, db_url)
    return result

@task
def get_api_token(environment: str) -> str:
    """
    Get API Token

    Args:
        environment (str): Environment

    Returns:
        str: API Token
    """
    api_url = prontuario_constants.API_URL.value.get(environment)
    api_username = get_secret_key.run(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_API_USERNAME.value,
        environment=environment
    )
    api_password = get_secret_key.run(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_API_PASSWORD.value,
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
        raise Exception(
            f"Error getting API token",
            f"({response.status_code}) - {response.json()}"
        )


@task
def get_scheduled_window() -> tuple[date, date]:
    """
    Get the scheduled window for a flow.

    Returns:
        tuple[date, date]: Window start and end dates
    """
    scheduled_start_time = prefect.context.get("scheduled_start_time")
    scheduled_date = scheduled_start_time.date()

    window_start = scheduled_date - timedelta(days=1)
    window_end = scheduled_date
    return window_start, window_end


@task
def transform_filter_invalid_cpf(dataframe:pd.DataFrame, cpf_column:str) -> pd.DataFrame:
    """
    Filter invalid CPFs

    Args:
        dataframe (pd.DataFrame): Dataframe to filter

    Returns:
        pd.DataFrame: Filtered dataframe
    """
    filtered_dataframe = dataframe[dataframe[cpf_column].apply(
        lambda cpf : CPF().validate(cpf)
    ) ]

    log(f"Filtered {dataframe.shape[0] - filtered_dataframe.shape[0]} invalid CPFs")

    return filtered_dataframe


@task
def transform_to_raw_format(json_data, cnes):
    """
    Transform data to raw format

    Args:
        json_data (list[dict]): Data to transform
        cnes (list[str]): CNES list

    Returns:
        dict: Transformed data
    """
    return {
        "data_list" : json_data,
        "cnes"      : cnes
    }


@task
def load_to_api(request_body, endpoint_name, api_token, environment):
    """
    Load data to raw endpoint

    Args:
        dataframe (pd.DataFrame): Data to load
        endpoint_name (str): Endpoint name
        identifier_column (str, optional): Identifier column. Defaults to "patient_cpf".
    """    
    api_url = prontuario_constants.API_URL.value.get(environment)
    request_response = requests.post(
        url     = f"{api_url}{endpoint_name}", 
        headers = {'Authorization': f'Bearer {api_token}'},
        timeout = 90,
        json    = request_body
    )

    if request_response.status_code != 201:
        raise Exception(
            f"Error loading data to endpoint {endpoint_name}",
            f"({request_response.status_code}) - {request_response.json()}"
        )