# -*- coding: utf-8 -*-
"""
Tasks for Raw Data
"""
import json
from datetime import timedelta, date
import pandas as pd

import prefect
from prefect import task
from prefect.client import Client

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
def extract_patient_data_from_db(
    db_url: str,
    time_window_start: date=None,
    time_window_duration: int=1,
) -> pd.DataFrame:
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
        time_window_end=time_window_start + timedelta(days=time_window_duration)
        query += f" WHERE timestamp >= '{time_window_start}' AND timestamp < '{time_window_end}'"

    patients=pd.read_sql(query, db_url)

    return patients

@task
def extract_cns_data_from_db(
    db_url: str,
    time_window_start: date=None,
    time_window_duration: int=1,
) -> pd.DataFrame:
    query = """
        SELECT
            cns, cns_provisorio
        FROM tb_cns_provisorios"""

    if time_window_start:
        time_window_end=time_window_start + timedelta(days=time_window_duration)
        query += f"""
            WHERE cns IN (
                SELECT cns
                FROM tb_pacientes
                WHERE timestamp >= '{time_window_start}' AND timestamp < '{time_window_end}'
            )"""

    patient_cns=pd.read_sql(query, db_url)

    return patient_cns


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
