# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Raw Data Extraction
"""
import json
from datetime import timedelta, date
import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.prontuarios.utils.validation import (
    is_valid_cpf
)
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.utils.tasks import (
    get_secret_key
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


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def extract_patient_data_from_db(
    db_url: str,
    time_window_start: date = None,
    time_window_duration: int = 1,
) -> pd.DataFrame:
    """
    Extracts patient data from the SMSRio database table using a (optional) time window.

    Args:
        db_url (str): The URL of the database.
        time_window_start (date, optional): The start date of the time window.
        time_window_duration (int, optional): The duration of the time window (in days).

    Returns:
        pd.DataFrame: A DataFrame containing the extracted patient data.
    """
    query = """
        SELECT
            cns, cpf as patient_cpf, nome, nome_mae, nome_pai, dt_nasc, sexo,
            racaCor, nacionalidade, obito, dt_obito,
            end_tp_logrado_cod, end_logrado, end_numero, end_comunidade,
            end_complem, end_bairro, end_cep, cod_mun_res, uf_res,
            cod_mun_nasc, uf_nasc, cod_pais_nasc,
            telefone, email, tp_telefone
        FROM tb_pacientes"""
    if time_window_start:
        time_window_end = time_window_start + timedelta(days=time_window_duration)
        query += f" WHERE timestamp >= '{time_window_start}' AND timestamp < '{time_window_end}'"

    patients = pd.read_sql(query, db_url)

    return patients


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def extract_cns_data_from_db(
    db_url: str,
    time_window_start: date = None,
    time_window_duration: int = 1,
) -> pd.DataFrame:
    """
    Extracts CNS data from the SMSRio database table using a (optional) time window.

    Args:
        db_url (str): The URL of the database.
        time_window_start (date, optional): The start date of the time window.
        time_window_duration (int, optional): The duration of the time window in days.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted CNS data.
    """
    query = """
        SELECT
            cns, cns_provisorio
        FROM tb_cns_provisorios"""

    if time_window_start:
        time_window_end = time_window_start + timedelta(days=time_window_duration)
        query += f"""
            WHERE cns IN (
                SELECT cns
                FROM tb_pacientes
                WHERE timestamp >= '{time_window_start}' AND timestamp < '{time_window_end}'
            )"""

    patient_cns = pd.read_sql(query, db_url)

    return patient_cns


@task
def transform_data_to_json(dataframe, identifier_column="patient_cpf") -> list[dict]:
    """
    Transform dataframe to json, exposing the identifier_column as a field

    Args:
        dataframe (pd.DataFrame): Dataframe to transform
        identifier_column (str): Identifier column

    Returns:
        list (list[dict]): List of jsons
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
def transform_merge_patient_and_cns_data(
    patient_data: pd.DataFrame,
    cns_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge patient and cns data into one instance

    Args:
        patient_data (pd.DataFrame): Patient data
        cns_data (pd.DataFrame): CNS data

    Returns:
        pd.DataFrame: Updated data with temporary cns in column 'cns_provisorios'
    """
    def get_all_patient_cns_values(cns):
        """
        Retrieves all the values of 'cns_provisorio' for a given CNS number.

        Args:
            cns (str): The CNS number to search for.

        Returns:
            list: A list of 'cns_provisorio' values associated with the given CNS number.
                  Returns an empty list if no matching rows are found.
        """
        patient_cns_rows = cns_data[cns_data['cns'] == cns]

        if patient_cns_rows.empty:
            return []

        return patient_cns_rows['cns_provisorio'].tolist()

    patient_data['cns_provisorios'] = patient_data['cns'].apply(get_all_patient_cns_values)

    return patient_data


@task
def transform_filter_invalid_cpf(dataframe: pd.DataFrame, cpf_column: str) -> pd.DataFrame:
    """
    Filter rows with valid CPFs

    Args:
        dataframe (pd.DataFrame): Dataframe to filter

    Returns:
        pd.DataFrame: Filtered dataframe with only valid CPF rows
    """
    filtered_dataframe = dataframe[dataframe[cpf_column].apply(is_valid_cpf)]

    log(f"Filtered {dataframe.shape[0] - filtered_dataframe.shape[0]} invalid CPFs")

    return filtered_dataframe
