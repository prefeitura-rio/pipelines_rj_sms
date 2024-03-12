# -*- coding: utf-8 -*-
from typing import Tuple
from sqlalchemy import create_engine, text

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
import sqlalchemy

from pipelines.prontuarios.std.formatters.generic.patient import (
    clean_none_records,
    drop_invalid_records,
    merge_keys,
    prepare_to_load,
)

from pipelines.prontuarios.std.formatters.smsrio.patient import (
    standardize_address_data,
    standardize_cns_list,
    standardize_decease_info,
    standardize_nationality,
    standardize_parents_names,
    standardize_race,
    standardize_telecom_data,
)


@task
def get_data_from_db(USER: str,
                     PASSWORD: str,
                     IP: str,
                     DATABASE: str,
                     date_range: list) -> pd.DataFrame:
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

    start_datetime = date_range["start_datetime"]
    end_datetime = date_range["end_datetime"]
    data_source = date_range["datasource_system"]

    log(f'Getting data between [ {start_datetime} , {end_datetime} ) from {data_source}')

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
            WHERE source_updated_at >= '{start_datetime}'
            AND source_updated_at <= '{end_datetime}'
        """)
        df_patients = pd.read_sql_query(query, conn)

    df_patients['id'] = df_patients['id'].astype(str)
    patients_json = df_patients.to_dict(orient='records')

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
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "datasource_system": "smsrio",
    }


def define_constants() -> Tuple[list, dict, dict, dict, dict]:
    """
    Creating constants as global variables

    Returns:
        lista_campos_api (list) : List of API acceptable parameters
        logradouros_dict (dict) : From/to dictionary to normalize logradouros
        city_dict (dict) : From/to dictionary to normalize city
        state_dict (dict) : From/to dictionary to normalize state
        country_dict (dict) : From/to dictionary to normalize country
    """
    lista_campos_api = [
        "active",
        "birth_city_cod",
        "birth_state_cod",
        "birth_country_cod",
        "birth_date",
        "patient_cpf",
        "deceased",
        "deceased_date",
        "father_name",
        "gender",
        "mother_name",
        "name",
        "nationality",
        "protected_person",
        "race",
        "cns_list",
        "address_list",
        "telecom_list",
        "raw_source_id",
        "patient_code"
    ]

    logradouros = pd.read_csv(
        "pipelines/prontuarios/std/logradouros.csv", dtype={"Número_DNE": str, "Nome": str}
    )
    logradouros_dict = dict(zip(logradouros["Número_DNE"], logradouros["Nome"]))

    city = pd.read_csv(
        "pipelines/prontuarios/std/municipios.csv", dtype={"COD UF": str, "COD": str, "NOME": str}
    )
    city_dict = dict(zip(city["COD"].str.slice(start=0, stop=-1), city["COD"]))

    state = pd.read_csv(
        "pipelines/prontuarios/std/estados.csv", dtype={"COD": str, "NOME": str, "SIGLA": str}
    )
    state_dict = dict(zip(state["SIGLA"], state["COD"]))

    countries = pd.read_csv(
        "pipelines/prontuarios/std/paises.csv", dtype={"country": str, "code": str}
    )
    country_dict = dict(zip(countries["code"], countries["code"]))

    return lista_campos_api, logradouros_dict, city_dict, state_dict, country_dict


@task
def format_json(json_list: list) -> list:
    """
    Drop invalid patient records, i.e. records without valid inputs on required fields

    Args:
        json_list (list): Payload from raw API
    Returns:
        json_list_notna (list): Valid patient inputs ready to standardize
    """
    log(f"Starting flow with {len(json_list)} registers")

    json_list_merged = list(map(merge_keys, json_list))
    json_list_valid = list(map(drop_invalid_records, json_list_merged))
    json_list_notna = clean_none_records(json_list_valid)

    log(
        f"{len(json_list) - len(json_list_notna)} registers dropped,\
        standardizing remaining {len(json_list_notna)} registers"
    )

    return json_list_notna


@task
def standartize_data(
    raw_data: list,
    logradouros_dict: dict,
    city_dict: dict,
    state_dict: dict,
    country_dict: dict,
    lista_campos_api: list,
) -> list:
    """
    Standardize fields and prepare to post

    Args:
        raw_data (list): Raw data to be standardized
        city_name_dict (dict): From/to dictionary to code city names
        state_dict (dict): From/to dictionary to code state names
        country_dict (dict): From/to dictionary to code country names
    Returns:
        list: Data ready to load to API
    """
    log("Starting standardization of race field")
    patients_json_std_race = list(map(standardize_race, raw_data))

    log("Starting standardization of nationality field")
    patients_json_std_nationality = list(map(standardize_nationality, patients_json_std_race))

    log("Starting standardization of parents name field")
    patients_json_std_parents = list(map(standardize_parents_names, patients_json_std_nationality))

    log("Starting standardization of cns field")
    patients_json_std_cns = list(map(standardize_cns_list, patients_json_std_parents))

    log("Starting standardization of decease field")
    patients_json_std_decease = list(map(standardize_decease_info, patients_json_std_cns))

    log("Starting standardization of address field")
    patients_json_std_address = list(
        map(
            lambda p: standardize_address_data(
                data=p,
                logradouros_dict=logradouros_dict,
                city_dict=city_dict,
                state_dict=state_dict,
                country_dict=country_dict,
            ),
            patients_json_std_decease,
        )
    )

    log("Starting standardization of telecom field")
    patients_json_std_telecom = list(map(standardize_telecom_data, patients_json_std_address))

    log("Preparing data to load")
    patients_json_std_clean = list(map(lambda x: prepare_to_load(x), patients_json_std_telecom))

    return patients_json_std_clean


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
    # df['id'] = [str(uuid.uuid4()) for i in range(len(df))]
    df.rename({'birth_city_cod': 'birth_city_id',
               'birth_state_cod': 'birth_state_id',
               'birth_country_cod': 'birth_country_id'}, axis=1, inplace=True)

    with engine.begin() as conn:
        df.to_sql('std__patientrecord',
                  con=conn,
                  index=False,
                  if_exists='append',
                  dtype={"cns_list": sqlalchemy.types.JSON,
                         "address_list": sqlalchemy.types.JSON,
                         "telecom_list": sqlalchemy.types.JSON},
                  method='multi')
