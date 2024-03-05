from sqlalchemy import create_engine,text
import pandas as pd
from prefect import task
import uuid
import sqlalchemy

from prefeitura_rio.pipelines_utils.logging import log

@task
def get_data_from_db(USER: str,
                     PASSWORD: str,
                     IP: str,
                     DATABASE: str,
                     request_params: dict) -> pd.DataFrame:
    """
    Args:
        USER (str):
        PASSWORD (str):
        IP (str):
        DATABASE (str):
        request_params (dict):

    Returns:
        pd.Dataframe:
    """
    engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{IP}:5432/{DATABASE}")

    data_source = request_params['datasource_system']
    start_datetime = request_params['start_datetime']
    end_datetime = request_params['end_datetime']
    
    log(f'Getting data between {start_datetime} and {end_datetime} from {data_source}')

    with engine.begin() as conn:
        query = text(f"""
            SELECT p.*
            FROM public.raw__patientrecord as p
            INNER JOIN (
                    SELECT DISTINCT cnes
                    FROM public.datasource
                    WHERE system = '{data_source}'
            ) as cnes_vitai
            ON cnes_vitai.cnes = p.data_source_id
            WHERE created_at > '{start_datetime}'
            AND created_at < '{end_datetime}'
        """)
        df_patients = pd.read_sql_query(query, conn)

    df_patients['id'] = df_patients['id'].astype(str)
    patients_json = df_patients.to_dict(orient='records')
    
    return patients_json

@task
def insert_data_to_db(USER: str,
                     PASSWORD: str,
                     IP: str,
                     DATABASE: str,
                     data: list):
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
    df['id'] = [str(uuid.uuid4()) for i in range(len(df))]
    df.rename({'birth_city_cod':'birth_city_id',
           'birth_state_cod':'birth_state_id',
           'birth_country_cod':'birth_country_id'},axis=1,inplace=True)
    
    with engine.begin() as conn:
        df.to_sql('std__patientrecord', 
                con=conn, 
                index=False, 
                if_exists='append',
                dtype={"cns_list": sqlalchemy.types.JSON,
                        "address_list": sqlalchemy.types.JSON,
                        "telecom_list":sqlalchemy.types.JSON}, 
                method='multi')