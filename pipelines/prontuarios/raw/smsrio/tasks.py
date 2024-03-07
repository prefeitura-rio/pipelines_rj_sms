# -*- coding: utf-8 -*-
"""
Tasks for SMSRio Raw Data Extraction
"""
import json
from datetime import date, timedelta

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.utils.tasks import load_to_api, transform_to_raw_format
from pipelines.prontuarios.utils.validation import is_valid_cpf
from pipelines.utils.tasks import get_secret_key


@task
def get_smsrio_database_url(environment, squema: str = "sms_pacientes"):
    """
    Get SMSRio database url from Infisical Secrets

    Args:
        environment (str): Environment
        squema (str): Database schema

    Returns:
        str: Database url
    """
    database_url = get_secret_key.run(
        secret_path=smsrio_constants.INFISICAL_PATH.value,
        secret_name=smsrio_constants.INFISICAL_DB_URL.value,
        environment=environment,
    )
    return f"{database_url}/{squema}"


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
        SELECT cpf as patient_cpf, nome, nome_mae, nome_pai, dt_nasc, sexo,
            racaCor, nacionalidade, obito, dt_obito,
            end_tp_logrado_cod, end_logrado, end_numero, end_comunidade,
            end_complem, end_bairro, end_cep, cod_mun_res, uf_res,
            cod_mun_nasc, uf_nasc, cod_pais_nasc,
            telefone, email, tp_telefone,
            json_arrayagg(tb_cns_provisorios.cns_provisorio) as cns_provisorio
        FROM tb_pacientes
            INNER JOIN tb_cns_provisorios ON tb_pacientes.cns = tb_cns_provisorios.cns"""
    if time_window_start:
        time_window_end = time_window_start + timedelta(days=time_window_duration)
        query += f"""
            WHERE tb_pacientes.timestamp >= '{time_window_start}'
                AND tb_pacientes.timestamp < '{time_window_end}'"""

    query += " GROUP BY tb_pacientes.id"

    patients = pd.read_sql(query, db_url)

    return patients


@task()
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

    output = []
    for _, row in dataframe.iterrows():
        row_as_json = row.to_json(date_format="iso")
        output.append({identifier_column: row[identifier_column], "data": json.loads(row_as_json)})

    return output


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


@task(max_retries=3, retry_delay=timedelta(minutes=3))
def load_patient_data_to_api(patient_data: pd.DataFrame, environment: str, api_token: str):
    """
    Loads patient data to the API.

    Args:
        patient_data (pd.DataFrame): The patient data to be loaded.
        environment (str): The environment to which the data will be loaded.
        api_token (str): The API token for authentication.

    Returns:
        None
    """

    json_data_batches = transform_data_to_json.run(dataframe=patient_data)

    request_bodies = transform_to_raw_format.run(
        json_data=json_data_batches, cnes=smsrio_constants.SMSRIO_CNES.value
    )

    load_to_api.run(
        request_body=request_bodies,
        endpoint_name="raw/patientrecords",
        api_token=api_token,
        environment=environment,
    )
