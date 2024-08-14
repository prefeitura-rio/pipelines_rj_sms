# -*- coding: utf-8 -*-
# flake8: noqa
"""
Tasks for SMSRio Raw Data Extraction
"""
import json
from datetime import date, timedelta

import pandas as pd
from sqlalchemy.exc import InternalError

from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.utils.misc import build_additional_fields
from pipelines.prontuarios.utils.tasks import load_to_api, transform_to_raw_format
from pipelines.prontuarios.utils.validation import is_valid_cpf
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key, load_table_from_database


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
    time_window_end: date = None,
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
    log(f"Extracting patient data from {time_window_start} to {time_window_end}")
    patients = load_table_from_database.run(
        db_url=db_url,
        query=f"""
            SELECT
                id as source_id,
                cpf as patient_cpf, nome,nome_mae,
                nome_pai, dt_nasc, sexo, racaCor,
                nacionalidade, obito, dt_obito,
                end_tp_logrado_cod, end_logrado, end_numero,
                end_comunidade, end_complem, end_bairro,
                end_cep, cod_mun_res,
                uf_res, cod_mun_nasc, uf_nasc, cod_pais_nasc, email,
                timestamp, cns, telefone
            FROM tb_pacientes
            WHERE timestamp BETWEEN '{time_window_start}' AND '{time_window_end}';
        """,
    )
    log(f"Extracted {patients.shape[0]} patients")

    log(f"Extracting CNS data from {time_window_start} to {time_window_end}")
    cnss = load_table_from_database.run(
        db_url=db_url,
        query=f"""
            SELECT
                cns,
                JSON_ARRAY_APPEND(JSON_ARRAYAGG(cns_provisorio), '$', cns) as cns_provisorio
            FROM tb_cns_provisorios
            WHERE
                cns IN (
                    SELECT tb_pacientes.cns
                    FROM tb_pacientes
                    WHERE tb_pacientes.timestamp BETWEEN '{time_window_start}' AND '{time_window_end}'
                )
            GROUP BY cns;
        """,
    )
    log(f"Extracted {cnss.shape[0]} CNS data")

    log(f"Extracting Telefone data from {time_window_start} to {time_window_end}")
    telefones = load_table_from_database.run(
        db_url=db_url,
        query=f"""
            SELECT
                cns,
                JSON_ARRAYAGG(telefone) as telefones
            from tb_pacientes_telefones
            WHERE cns IN (
                SELECT tb_pacientes.cns
                FROM tb_pacientes
                WHERE tb_pacientes.timestamp BETWEEN  '{time_window_start}' AND '{time_window_end}'
            )
            GROUP BY cns;
        """,
    )
    log(f"Extracted {telefones.shape[0]} Telefone data")

    patients = patients.merge(cnss, on="cns", how="left").merge(telefones, on="cns", how="left")

    def handle_cns(cns):
        if cns:
            try:
                cns = json.loads(cns)
            except TypeError:
                cns = []
        else:
            cns = []
        return json.dumps(cns[::-1])

    patients["cns_provisorio"] = patients["cns_provisorio"].apply(handle_cns)

    def join_phones(row):
        main_phone = row["telefone"]
        try:
            phone_list = json.loads(row["telefones"])
        except TypeError:
            phone_list = []
        if main_phone:
            phone_list.append(main_phone)
        return json.dumps(phone_list[::-1])

    patients["telefones"] = patients.apply(join_phones, axis=1)

    return patients


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
    json_data = json.loads(patient_data.to_json(orient="records", date_format="iso"))

    json_data_batches = build_additional_fields(
        data_list=json_data,
        cpf_get_function=lambda data: data["patient_cpf"],
        birth_data_get_function=lambda data: data["dt_nasc"],
        source_updated_at_get_function=lambda data: data["timestamp"],
        source_id_get_function=lambda data: data["source_id"],
    )

    request_bodies = transform_to_raw_format.run(
        json_data=json_data_batches, cnes=smsrio_constants.SMSRIO_CNES.value
    )

    load_to_api.run(
        request_body=request_bodies,
        endpoint_name="raw/patientrecords",
        api_token=api_token,
        environment=environment,
    )
