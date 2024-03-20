# -*- coding: utf-8 -*-

import pandas as pd
import sqlalchemy
from prefeitura_rio.pipelines_utils.logging import log
from sqlalchemy import create_engine, text

from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_data_from_db(
    USER: str, PASSWORD: str, IP: str, DATABASE: str, date_range: list
) -> pd.DataFrame:
    """
    Get patient data from database connector between date range specified

    Args:
        USER (str): Database USER credential
        PASSWORD (str): Database PASSWORD credential
        IP (str): Database IP credential
        DATABASE (str): Database DATABASE credential
        request_params (dict): Date range ans source filter

    Returns:
        pd.Dataframe: Raw data to be standardazided
    """
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{IP}:5432/{DATABASE}")

    start_datetime = date_range["source_start_datetime"]
    end_datetime = date_range["source_end_datetime"]
    data_source = date_range["datasource_system"]

    log(f"Getting data between [ {start_datetime} , {end_datetime} ) from {data_source}")

    with engine.begin() as conn:
        query = text(
            f"""
            SELECT p.*
            FROM public.raw__patientrecord as p
            INNER JOIN (
                    SELECT DISTINCT cnes
                    FROM public.datasource
                    WHERE system = '{data_source}'
            ) as cnes_vitai
            ON cnes_vitai.cnes = p.data_source_id
            WHERE source_updated_at >= '{start_datetime}'
            AND source_updated_at <= '{end_datetime}'
        """
        )
        df_patients = pd.read_sql_query(query, conn)

    df_patients["id"] = df_patients["id"].astype(str)
    patients_json = df_patients.to_dict(orient="records")

    return patients_json


@task
def get_params(start_datetime: str, end_datetime: str) -> dict:
    """
    Creating params dict

    Args:
        start_datetime (str) : Initial date extraction
        end_datetime (str) : Final date extraction

    Returns:
        dict : Params dictionary
    """
    return {
        "source_start_datetime": start_datetime,
        "source_end_datetime": end_datetime,
        "datasource_system": "smsrio",
    }


@task
def insert_data_to_db(USER: str, PASSWORD: str, IP: str, DATABASE: str, data: list):
    """
    Args:
        USER (str):
        PASSWORD (str):
        IP (str):
        DATABASE (str):
        data_source (str):
        start_datetime (str):
        end_datetime (str):

    Returns:
        pd.Dataframe:
    """
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{IP}:5432/{DATABASE}")

    df = pd.DataFrame(data)
    # df['id'] = [str(uuid.uuid4()) for i in range(len(df))]
    df.rename(
        {
            "birth_city_cod": "birth_city_id",
            "birth_state_cod": "birth_state_id",
            "birth_country_cod": "birth_country_id",
        },
        axis=1,
        inplace=True,
    )

    with engine.begin() as conn:
        df.to_sql(
            "std__patientrecord",
            con=conn,
            index=False,
            if_exists="append",
            dtype={
                "cns_list": sqlalchemy.types.JSON,
                "address_list": sqlalchemy.types.JSON,
                "telecom_list": sqlalchemy.types.JSON,
            },
            method="multi",
        )
