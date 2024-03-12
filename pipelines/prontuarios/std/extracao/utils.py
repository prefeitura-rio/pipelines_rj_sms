from sqlalchemy import create_engine, text
import pandas as pd
from prefect import task
from itertools import cycle
from pipelines.prontuarios.std.extracao.smsrio.flows import smsrio_standardization_historical
from pipelines.prontuarios.std.extracao.vitai.flows import vitai_standardization_historical

from prefeitura_rio.pipelines_utils.logging import log


@task
def run_flow_smsrio(datetime_range_list: list,
                    DATABASE: str,
                    USER: str,
                    PASSWORD: str,
                    IP: str):
    """
    Args:
        datetime_range_list (list): List of date ranges to iterate with std flow
        DATABASE (str): Database DATABASE credential
        USER (str): Database USER credential
        PASSWORD (str): Database PASSWORD credential
        IP (str): Database IP credential
    """

    for start_datetime, end_datetime, _ in datetime_range_list:
        params = {"start_datetime": start_datetime,
                  "end_datetime": end_datetime,
                  "database": DATABASE,
                  "user": USER,
                  "password": PASSWORD,
                  "ip": IP,
                  "run_on_schedule": False}

        smsrio_standardization_historical.run(**params)

@task
def run_flow_vitai(datetime_range_list: list,
                    DATABASE: str,
                    USER: str,
                    PASSWORD: str,
                    IP: str):
    """
    Args:
        datetime_range_list (list): List of date ranges to iterate with std flow
        DATABASE (str): Database DATABASE credential
        USER (str): Database USER credential
        PASSWORD (str): Database PASSWORD credential
        IP (str): Database IP credential
    """

    for start_datetime, end_datetime, _ in datetime_range_list:
        params = {"start_datetime": start_datetime,
                  "end_datetime": end_datetime,
                  "database": DATABASE,
                  "user": USER,
                  "password": PASSWORD,
                  "ip": IP,
                  "run_on_schedule": False}

        vitai_standardization_historical.run(**params)


@task
def get_datetime_in_range(USER: str,
                          PASSWORD: str,
                          IP: str,
                          DATABASE: str,
                          request_params: dict,
                          system: str) -> list:
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

    if ('start_datetime' in request_params.keys()) & ('end_datetime' in request_params.keys()):
        range_clause = f"""
        WHERE p.source_updated_at >= '{request_params['start_datetime']}'
        AND p.source_updated_at <= '{request_params['end_datetime']}'
        """
        log(f"Getting data between { request_params['start_datetime'] } and { request_params['end_datetime'] } from {system}")

    elif 'start_datetime' in request_params.keys():
        range_clause = f"WHERE p.source_updated_at >= '{request_params['start_datetime']}' "
        log(f"Getting data starting in {request_params['start_datetime']} from {system}")

    elif 'end_datetime' in request_params.keys():
        range_clause = f"WHERE p.source_updated_at <= '{request_params['end_datetime']}' "
        log(f"Getting data until {request_params['end_datetime']} from {system}")

    else:
        range_clause = ''
        log(f"Getting all data from {system}")

    with engine.begin() as conn:  # precisa lidar melhor com o range 23h-0h
        query = text(f"""
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
        """)
        df_range_data = pd.read_sql_query(query, conn)
        list_range_data = list(
            zip(df_range_data['data_inicio'], df_range_data['data_fim'], cycle([system])))

        return list_range_data
