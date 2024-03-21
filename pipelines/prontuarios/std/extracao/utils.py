# -*- coding: utf-8 -*-

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from sqlalchemy import create_engine, text

from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_datetime_in_range(
    USER: str,
    PASSWORD: str,
    IP: str,
    DATABASE: str,
    ENVIROMENT: str,
    request_params: dict,
    system: str,
) -> list:
    """
    Get list of date ranges to iterate with standardization flow.
    Considering range data as [start_datetime,end_datetime)
    Args:
        USER (str): Database USER credential
        PASSWORD (str): Database PASSWORD credential
        IP (str): Database IP credential
        DATABASE (str): Database DATABASE credential
        request_params (dict): Date range ans source filter
    Returns:
        list: List of days to iterate with std flow
    """

    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{IP}:5432/{DATABASE}")

    if ("source_start_datetime" in request_params.keys()) & (
        "source_end_datetime" in request_params.keys()
    ):
        range_clause = f"""
        WHERE p.source_updated_at >= '{request_params['source_start_datetime']}'
        AND p.source_updated_at <= '{request_params['source_end_datetime']}'
        """
        log(
            f"""
            Getting data between { request_params['source_start_datetime'] }
            and { request_params['source_end_datetime'] } from {system}
            """
        )

    elif "start_datetime" in request_params.keys():
        range_clause = f"WHERE p.source_updated_at >= '{request_params['source_start_datetime']}' "
        log(f"Getting data starting in {request_params['source_start_datetime']} from {system}")

    elif "end_datetime" in request_params.keys():
        range_clause = f"WHERE p.source_updated_at <= '{request_params['source_end_datetime']}' "
        log(f"Getting data until {request_params['source_end_datetime']} from {system}")

    else:
        range_clause = ""
        log(f"Getting all data from {system}")

    with engine.begin() as conn:  # precisa lidar melhor com o range 23h-0h
        query = text(
            f"""
            SELECT DISTINCT
            CAST(p.source_updated_at  AS DATE) as data_inicio,
            CAST(p.source_updated_at + INTERVAL '1 day' AS DATE) as data_fim
            FROM raw__patientrecord as p
            INNER JOIN (
                        SELECT DISTINCT cnes
                        FROM public.datasource
                        WHERE system = '{system}'
            ) as cnes_vitai
            ON cnes_vitai.cnes = p.data_source_id
            {range_clause}
        """
        )
        df_range_data = pd.read_sql_query(query, conn)
        params_list = []
        for dt_inicio, dt_fim in list(zip(df_range_data["data_inicio"], df_range_data["data_fim"])):
            params = {
                "source_start_datetime": str(dt_inicio),
                "source_end_datetime": str(dt_fim),
                "database": DATABASE,
                "user": USER,
                "password": PASSWORD,
                "ip": IP,
                "run_on_schedule": False,
                "enviroment": ENVIROMENT,
            }
            params_list.append(params)

        return params_list
