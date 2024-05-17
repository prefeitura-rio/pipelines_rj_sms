# -*- coding: utf-8 -*-
from datetime import date, timedelta

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


@task
def get_inserted_registers(target_date: date, db_url: str) -> pd.DataFrame:
    """
    Retrieves the inserted raw registers from the database within a specified time window.

    Args:
        db_url (str): The URL of the database.
        time_window_start (date, optional): The start date of the time window.
        time_window_duration (int, optional): The duration of the time window in days.

    Returns:
        pd.DataFrame: A DataFrame containing the inserted raw registers.
    """
    conditions = pd.read_sql(
        f"""
        select
            d.system, raw.patient_code,
            raw.source_updated_at as event_moment,
            raw.created_at as raw_acquisition_moment,
            std.created_at as standardization_moment,
            'condition' as entity
        from raw__patientcondition raw
            inner join datasource d on d.cnes = raw.data_source_id
            left join public.std__patientcondition std on raw.id = std.raw_source_id
        where date(raw.created_at) = '{target_date}'
        order by raw.source_updated_at desc
        """,
        db_url,
    )
    patient = pd.read_sql(
        f"""
        select
            d.system, raw.patient_code,
            raw.source_updated_at as event_moment,
            raw.created_at as raw_acquisition_moment,
            std.created_at as standardization_moment,
            mrg_patient.created_at as merge_moment,
            'patient' as entity
        from raw__patientrecord raw
            inner join datasource d on d.cnes = raw.data_source_id
            left join public.std__patientrecord std on raw.id = std.raw_source_id
            left join public.patient as mrg_patient on mrg_patient.patient_cpf = std.patient_cpf
        where date(raw.created_at) = '{target_date}'
        order by raw.source_updated_at desc
        """,
        db_url,
    )
    return pd.concat([conditions, patient], ignore_index=True)


@task
def create_report(target_date: date, data: pd.DataFrame) -> None:
    """
    Creates a raw report from the raw data.

    Args:
        data (pd.DataFrame): The dataframe.

    Returns:
        None
    """
    formatted_date = target_date.strftime("%d/%m/%Y")

    metrics = (
        data.groupby(by=["system", "entity"])
        .count()
        .reset_index()[["entity", "system", "raw_acquisition_moment", "standardization_moment", "merge_moment"]]
        .rename(
            columns={
                "raw_acquisition_moment": "RAW",
                "standardization_moment": "STD",
                "merge_moment": "MRG",
                "system": "Fonte de Dados",
                "entity": "Entidade",
            }
        )
    )

    metrics["Status RAW->STD"] = metrics.apply(
        func=lambda x: "✅" if x["RAW"] == x["STD"] else "❌", axis=1
    )
    metrics["Status STD->MRG"] = metrics.apply(
        func=lambda x: "✅" if x["STD"] == x["MRG"] else "❌", axis=1
    )

    send_message(
        title=f"Ingestão Diária de Dados Brutos: {formatted_date}",
        message=f"""Quantidade de Dados Inseridos na base:
        ```{metrics.to_markdown(index=False)}```
        """,
        monitor_slug="data-ingestion",
    )
    return None
