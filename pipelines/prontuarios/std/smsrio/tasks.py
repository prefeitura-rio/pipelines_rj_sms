# -*- coding: utf-8 -*-
from datetime import timedelta
from typing import Tuple

import pandas as pd
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

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
def get_params(start_datetime: str) -> dict:
    """
    Creating params
    Args:
        start_datetime (str) : initial date extraction

    Returns:
        dict : params dictionary
    """
    end_datetime = start_datetime + timedelta(days=1)
    return {
        "source_start_datetime": start_datetime.strftime("%Y-%m-%d 00:00:00"),
        "source_end_datetime": end_datetime.strftime("%Y-%m-%d 00:00:00"),
        "datasource_system": "smsrio",
    }


@task
def define_constants() -> Tuple[list, dict, dict, dict, dict]:
    """
    Creating constants as global variables

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
    patients_json_std = list(map(standardize_race, raw_data))

    log("Starting standardization of nationality field")
    patients_json_std = list(map(standardize_nationality, patients_json_std))

    log("Starting standardization of parents name field")
    patients_json_std = list(map(standardize_parents_names, patients_json_std))

    log("Starting standardization of cns field")
    patients_json_std = list(map(standardize_cns_list, patients_json_std))

    log("Starting standardization of decease field")
    patients_json_std = list(map(standardize_decease_info, patients_json_std))

    log("Starting standardization of address field")
    patients_json_std = list(
        map(
            lambda p: standardize_address_data(
                data=p,
                logradouros_dict=logradouros_dict,
                city_dict=city_dict,
                state_dict=state_dict,
                country_dict=country_dict,
            ),
            patients_json_std,
        )
    )

    log("Starting standardization of telecom field")
    patients_json_std = list(map(standardize_telecom_data, patients_json_std))

    log("Preparing data to load")
    patients_json_std = list(map(lambda x: prepare_to_load(x), patients_json_std))

    return patients_json_std
