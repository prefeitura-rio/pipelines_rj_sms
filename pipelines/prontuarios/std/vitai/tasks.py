# -*- coding: utf-8 -*-
from typing import Tuple

# from unidecode import unidecode
# from unidecode import unidecode
import pandas as pd
from prefect import task

from pipelines.prontuarios.std.vitai.utils import (
    clean_none_records,
    clean_records_fields,
    drop_invalid_records,
    merge_keys,
    standardize_address_data,
    standardize_cns_list,
    standardize_decease_info,
    standardize_nationality,
    standardize_parents_names,
    standardize_race,
    standardize_telecom_data,
)


@task
def get_params(start_datetime: str, end_datetime: str) -> dict:
    """
    Creating params
    Args:
        start_datetime (str) : initial date extraction
        end_datetime (str) : final date extraction

    Returns:
        dict : params dictionary
    """
    return {
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "datasource_system": "vitai",
    }


@task
def define_constants() -> Tuple[list, dict, dict, dict]:
    """
    Creating constants as flobal variables

    Returns:
        lista_campos_api (list) : list of API acceptable parameters
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
    ]

    city = pd.read_csv(
        "pipelines/prontuarios/std/municipios.csv", dtype={"COD UF": str, "COD": str, "NOME": str}
    )
    city_name_dict = dict(zip(city["NOME"], city["COD"]))

    state = pd.read_csv(
        "pipelines/prontuarios/std/estados.csv", dtype={"COD": str, "NOME": str, "SIGLA": str}
    )
    state_dict = dict(zip(state["SIGLA"], state["COD"]))

    countries = pd.read_csv(
        "pipelines/prontuarios/std/paises.csv", dtype={"country": str, "code": str}
    )
    country_dict = dict(zip(countries["code"], countries["code"]))

    return lista_campos_api, city_name_dict, state_dict, country_dict


@task
def format_json(json_list: list) -> list:

    json_list_merged = list(map(merge_keys, json_list))
    json_list_valid = list(map(drop_invalid_records, json_list_merged))
    json_list_notna = clean_none_records(json_list_valid)

    return json_list_notna


@task
def standartize_data(
    raw_data: list,
    city_name_dict: dict,
    state_dict: dict,
    country_dict: dict,
    lista_campos_api: list,
) -> list:

    patients_json_std_race = list(map(standardize_race, raw_data))

    patients_json_std_nationality = list(map(standardize_nationality, patients_json_std_race))

    patients_json_std_parents = list(map(standardize_parents_names, patients_json_std_nationality))

    patients_json_std_cns = list(map(standardize_cns_list, patients_json_std_parents))

    patients_json_std_decease = list(map(standardize_decease_info, patients_json_std_cns))

    patients_json_std_address = list(
        map(
            lambda p: standardize_address_data(
                data=p,
                city_name_dict=city_name_dict,
                state_dict=state_dict,
                country_dict=country_dict,
            ),
            patients_json_std_decease,
        )
    )

    patients_json_std_telecom = list(map(standardize_telecom_data, patients_json_std_address))

    patients_json_std_clean = list(
        map(lambda x: clean_records_fields(x, lista_campos_api), patients_json_std_telecom)
    )

    return patients_json_std_clean
