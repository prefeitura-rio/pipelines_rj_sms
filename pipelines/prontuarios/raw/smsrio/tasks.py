# -*- coding: utf-8 -*-
"""
Tasks for Raw Data
"""
import json

from datetime import timedelta, date
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.prontuarios.constants import constants as prontuario_constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.utils.tasks import (
    get_secret_key
)


import prefect
import pandas as pd
import requests

from datetime import date, timedelta


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
def get_api_url(environment: str) -> str:
    """
    Get API URL

    Args:
        environment (str): Environment

    Returns:
        str: API URL
    """
    return prontuario_constants.API_URL.value.get(environment)


@task
def get_api_token(api_url, api_username, api_password) -> str:
    """
    Get API Token

    Args:
        environment (str): Environment

    Returns:
        str: API Token
    """
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
def extract_data_from_csv(
    path: str
):
    """
    Extract data from csv

    Args:
        path (str): Path to csv file

    Returns:
        pd.DataFrame: Dataframe with the extracted data
    """
    return pd.read_csv(path, header=0)

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
def transform_standardize_columns_names(dataframe, columns_map):
    """
    Standardize columns names

    Args:
        dataframe (pd.DataFrame): Dataframe to standardize
        columns_map (dict): Columns map
    
    Returns:
        pd.DataFrame: Dataframe with standardized columns names
    """
    dataframe = dataframe.rename(columns=columns_map)
    return dataframe

@task
def transform_data_to_json(dataframe, identifier_column="patient_cpf"):
    assert identifier_column in dataframe.columns, "identifier_column column not found"
    data_list = []
    for _, row in dataframe.iterrows():
        row_as_json = row.to_json(date_format='iso')
        
        data_list.append(
            {
                identifier_column: row[identifier_column],
                "data": json.loads(row_as_json)
            }
        )
    return data_list

@task
def transform_create_input_batches(input, batch_size=250):
    """
    Create input batches

    Args:
        input (list): Input data
        batch_size (int, optional): Batch size. Defaults to 250.

    Returns:
        list: Input batches
    """
    return [input[i:i + batch_size] for i in range(0, len(input), batch_size)]


@task
def load_to_raw_endpoint(json_data, api_url, endpoint_name, api_token):
    """
    Load data to raw endpoint

    Args:
        dataframe (pd.DataFrame): Data to load
        endpoint_name (str): Endpoint name
        identifier_column (str, optional): Identifier column. Defaults to "patient_cpf".
    """    
    request_body = {
        "data_list" : json_data,
        "cnes"      : "999999"
    }
    request_response = requests.post(
        url     = f"{api_url}{endpoint_name}", 
        headers = {'Authorization': f'Bearer {api_token}'},
        timeout = 90,
        json    = request_body
    )

    if request_response.status_code != 200:
        raise Exception(f"Error loading data to endpoint {endpoint_name} ({request_response.status_code}) - {request_response.json()}")
    

@task
def transform_merge_patient_and_cns_data(patient_data, cns_data):
    """
    Merge patient and cns data

    Args:
        patient_data (pd.DataFrame): Patient data
        cns_data (pd.DataFrame): CNS data

    Returns:
        pd.DataFrame: Updated data with temporary cns in column 'cns_provisorios'
    """
    def get_all_patient_cns_values(cns):
        patient_cns_rows = cns_data[ cns_data['cns'] == cns ]

        if patient_cns_rows.empty:
            return []
        
        return patient_cns_rows['cns_provisorio'].tolist()

    patient_data['cns_provisorios'] = patient_data['cns'].apply(get_all_patient_cns_values)

    return patient_data

@task
def transform_convert_date_to_string(dataframe, date_columns):
    """
    Convert date columns to string

    Args:
        dataframe (pd.DataFrame): Dataframe to convert
        date_columns (list[str]): Date columns to convert

    Returns:
        pd.DataFrame: Dataframe with converted columns
    """
    for date_column in date_columns:
        dataframe[date_column] = dataframe[date_column].apply(lambda x: x.strftime("%Y-%m-%d"))
    return dataframe