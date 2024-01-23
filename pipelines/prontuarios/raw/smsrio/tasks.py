# -*- coding: utf-8 -*-
"""
Tasks for Raw Data
"""
import json

from prefect import task
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.utils.tasks import (
    get_secret_key
)


@task
def get_database_url(environment):
    """
    Get database url

    Args:
        environment (str): Environment
    
    Returns:
        str: Database url
    """
    database_url = get_secret_key.run(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_DB_URL.value,
        environment=environment
    )
    return database_url


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
    """
    Transform dataframe to json

    Args:
        dataframe (pd.DataFrame): Dataframe to transform
        identifier_column (str): Identifier column
    
    Returns:
        list: List of jsons
    """
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