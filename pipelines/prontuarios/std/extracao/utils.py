from sqlalchemy import create_engine, text
import pandas as pd
from prefect import task
from itertools import cycle
from pipelines.prontuarios.std.extracao.smsrio.flows import smsrio_standardization_historical

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
def get_datetime_in_range(USER: str,
                          PASSWORD: str,
                          IP: str,
                          DATABASE: str,
                          request_params: dict) -> list:
    """
    Get list of datetime ranges to iterate with standardization flow.
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

    data_source = request_params['datasource_system']
    start_datetime = request_params['start_datetime']
    end_datetime = request_params['end_datetime']

    log(f'Getting data between [ {start_datetime} , {end_datetime} ) from {data_source}')

    with engine.begin() as conn:  # precisa lidar melhor com o range 23h-0h
        query = text(f"""
            SELECT DISTINCT
            CONCAT(
                CAST(p.source_updated_at  AS DATE)
                ,' '
                ,LPAD(EXTRACT(HOUR FROM p.source_updated_at )::text,2::int,'0'::text)
                ,'\:00') as data_inicio,
            CONCAT(
                CAST(p.source_updated_at  AS DATE)
                ,' '
                ,CASE
                WHEN LPAD( (EXTRACT(HOUR FROM p.source_updated_at )+1)::text,
                             2::int,
                            0::text ) = '24'::text THEN '00'
                ELSE LPAD( (EXTRACT(HOUR FROM p.source_updated_at )+1)::text, 2::int, 0::text )
                END
                ,'\:00') as data_fim
            FROM raw__patientrecord as p
            INNER JOIN (
                        SELECT DISTINCT cnes
                        FROM public.datasource
                        WHERE system = '{data_source}'
            ) as cnes_vitai
            ON cnes_vitai.cnes = p.data_source_id
            WHERE source_updated_at  >= '{start_datetime}'
            AND source_updated_at  <= '{end_datetime}'
        """)
        df_range_data = pd.read_sql_query(query, conn)
        list_range_data = list(
            zip(df_range_data['data_inicio'], df_range_data['data_fim'], cycle([data_source])))

        return list_range_data
