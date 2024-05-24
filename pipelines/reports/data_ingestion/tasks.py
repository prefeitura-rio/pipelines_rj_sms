# -*- coding: utf-8 -*-
from datetime import date, timedelta
from typing import Literal

import pandas as pd

from pipelines.reports.data_ingestion.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import get_secret_key


@task
def get_prontuarios_database_url(environment):
    """
    Get Prontuarios database url from Infisical Secrets.

    Args:
        environment (str): Environment

    Returns:
        str: Database url
    """
    database_url = get_secret_key.run(
        secret_path="/",
        secret_name=constants.INFISICAL_PRONTUARIOS_DB_URL.value,
        environment=environment,
    )
    return database_url


@task
def get_target_date(custom_target_date: str) -> date:
    """
    Returns the target date based on the provided custom_target_date.

    Args:
        custom_target_date (str): The custom target date in ISO format (YYYY-MM-DD).

    Returns:
        date: The target date.

    Raises:
        ValueError: If the custom_target_date is not in the correct format.

    """
    if custom_target_date == "":
        return date.today() - timedelta(days=1)
    else:
        return date.fromisoformat(custom_target_date)


@task(retry_delay=timedelta(minutes=5), max_retries=3)
def get_raw_entity(
    target_date: date,
    db_url: str,
    entity_name: Literal["patientrecord", "patientcondition", "encounter"],
) -> pd.DataFrame:
    records = pd.read_sql(
        f"""
        SELECT 
            patient_code, 
            datasource.system,
            MAX(created_at) AS moment
        FROM raw__{entity_name}
            INNER JOIN datasource 
                on cnes = raw__{entity_name}.data_source_id
        WHERE DATE(created_at) = '{target_date}'
        GROUP BY patient_code, datasource.system;
        """,
        db_url,
    )
    return records


@task(retry_delay=timedelta(minutes=5), max_retries=3)
def get_std_entity(
    target_date: date,
    db_url: str,
    entity_name: Literal["patientrecord", "patientcondition", "encounter"],
) -> pd.DataFrame:
    records = pd.read_sql(
        f"""
        SELECT
            std__{entity_name}.patient_code,
            datasource.system,
            MAX(std__{entity_name}.created_at) AS moment
        FROM std__{entity_name}
            INNER JOIN raw__{entity_name} 
                on std__{entity_name}.raw_source_id = raw__{entity_name}.id
            INNER JOIN datasource 
                on cnes = raw__{entity_name}.data_source_id
        WHERE DATE(std__{entity_name}.created_at) = '{target_date}'
        GROUP BY std__{entity_name}.patient_code, datasource.system;
        """,
        db_url,
    )
    return records


@task(retry_delay=timedelta(minutes=5), max_retries=3)
def get_mrg_patientrecords(target_date: date, db_url: str) -> pd.DataFrame:
    records = pd.read_sql(
        f"""
        SELECT
            patient_code,
            MAX(patient.created_at) AS moment
        FROM patient
        WHERE DATE(created_at) = '{target_date}'
        GROUP BY patient_code;
        """,
        db_url,
    )
    return records


@task
def create_report(
    target_date: date,
    raw_patientrecord: pd.DataFrame,
    std_patientrecord: pd.DataFrame,
    mrg_patient: pd.DataFrame
) -> None:
    """
    Creates a raw report from the raw data.

    Args:
        data (pd.DataFrame): The dataframe.

    Returns:
        None
    """
    formatted_date = target_date.strftime("%d/%m/%Y")

    raw_patientrecord = {
        'total': raw_patientrecord.shape[0],
        'vitai': raw_patientrecord[raw_patientrecord.system == 'vitai'].shape[0],
        'vitacare': raw_patientrecord[raw_patientrecord.system == 'vitacare'].shape[0],
        'smsrio': raw_patientrecord[raw_patientrecord.system == 'smsrio'].shape[0],
    }
    std_patientrecord = {
        'total': std_patientrecord.shape[0],
        'vitai': std_patientrecord[std_patientrecord.system == 'vitai'].shape[0],
        'vitacare': std_patientrecord[std_patientrecord.system == 'vitacare'].shape[0],
        'smsrio': std_patientrecord[std_patientrecord.system == 'smsrio'].shape[0],
    }
    mrg_patient = {
        'total': mrg_patient.shape[0],
    }

    send_message(
        title=f"Ingestão Diária de Dados Brutos: {formatted_date}",
        message=f"""
## Pacientes
__Registros Criados no Dia:__
- **RAW**: +{raw_patientrecord['total']} registros no total
  - VITAI: +{raw_patientrecord['vitai']} registros
  - VITACARE: +{raw_patientrecord['vitacare']} registros
  - SMSRIO: +{raw_patientrecord['smsrio']} registros
- **STD**: +{std_patientrecord['total']} registros no total
  - VITAI: +{std_patientrecord['vitai']} registros
  - VITACARE: +{std_patientrecord['vitacare']} registros
  - SMSRIO: +{std_patientrecord['smsrio']} registros
- **MRG**: +{mrg_patient['total']} registros no total
""",
        monitor_slug="data-ingestion",
    )
    return None
