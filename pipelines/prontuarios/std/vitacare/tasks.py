# -*- coding: utf-8 -*-
from datetime import timedelta
from typing import Tuple

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.prontuarios.std.formatters.generic.patient import (
    clean_none_records,
    drop_invalid_records,
    merge_keys,
    prepare_to_load,
)
from pipelines.prontuarios.std.formatters.vitacare.patient import (
    standardize_address_data,
    standardize_cns_list,
    standardize_decease_info,
    standardize_nationality,
    standardize_parents_names,
    standardize_race,
    standardize_telecom_data,
)
from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_params(start_datetime: str, end_datetime: str) -> dict:
    """
    Creating params
    Args:
        start_datetime (str) : initial date extraction.

    Returns:
        dict : params dictionary
    """
    log(
        f"""
        Standardizing from {start_datetime}
        to {end_datetime}"""
    )
    return {
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "datasource_system": "vitacare",
    }


@task(nout=3)
def define_constants() -> Tuple[dict, dict, dict]:
    """
    Creating constants as global variables

    Returns:
        country_dict (dict) : From/to dictionary to normalize country
        city_dict_res (dict) : From/to dictionary to normalize city (used in address)
        city_dict_nasc (dict) : From/to dictionary to normalize city (used in birth_city)
    """

    city = pd.read_csv(
        "pipelines/prontuarios/std/municipios.csv", dtype={"COD UF": str, "COD": str, "NOME": str}
    )
    city_dict_nasc = dict(zip(city["COD"].str.slice(start=0, stop=-1), city["COD"]))
    city_dict_res = dict(zip(city["COD"], city["COD"]))

    countries = pd.read_csv(
        "pipelines/prontuarios/std/paises.csv", dtype={"country": str, "code": str}
    )
    country_dict = dict(zip(countries["country"].str.upper(), countries["code"]))

    return country_dict, city_dict_res, city_dict_nasc


@task(nout=2)
def format_json(json_list: list) -> list:
    """
    Drop invalid patient records, i.e. records without valid inputs on required fields
    Args:
        json_list (list): Payload from raw API
    Returns:
        json_list_notna (list): Valid patient inputs ready to standardize
    """

    print(f"Starting flow with {len(json_list)} registers")

    json_list_merged = list(map(merge_keys, json_list))
    json_list_info = list(map(drop_invalid_records, json_list_merged))
    json_list_valid, json_list_invalid = clean_none_records(json_list_info)

    print(
        f"{len(json_list_invalid)} registers dropped,\
        standardizing remaining {len(json_list_valid)} registers"
    )

    return json_list_valid, json_list_invalid


@task
def standartize_data(
    raw_data: list, city_dict_nasc: dict, city_dict_res: dict, country_dict: dict
) -> list:
    """
    Standardize fields and prepare to post
    Args:
        raw_data (list): Raw data to be standardized
        city_dict_nasc (dict): From/to dictionary to code city names (used in birth city)
        city_dict_res (dict): From/to dictionary to code city names (used in address)
        state_dict (dict): From/to dictionary to code state names
        country_dict (dict): From/to dictionary to code country names
    Returns:
        list: Data ready to load to API
    """

    print("Starting standardization of race field")
    patients_json_std_race = list(map(standardize_race, raw_data))

    print("Starting standardization of nationality field")
    patients_json_std_nationality = list(map(standardize_nationality, patients_json_std_race))

    print("Starting standardization of parents name field")
    patients_json_std_parents = list(map(standardize_parents_names, patients_json_std_nationality))

    print("Starting standardization of cns field")
    patients_json_std_cns = list(map(standardize_cns_list, patients_json_std_parents))

    print("Starting standardization of decease field")
    patients_json_std_decease = list(map(standardize_decease_info, patients_json_std_cns))

    print("Starting standardization of address field")
    patients_json_std_address = list(
        map(
            lambda p: standardize_address_data(
                data=p,
                city_dict_res=city_dict_res,
                city_dict_nasc=city_dict_nasc,
                country_dict=country_dict,
            ),
            patients_json_std_decease,
        )
    )

    print("Starting standardization of telecom field")
    patients_json_std_telecom = list(map(standardize_telecom_data, patients_json_std_address))

    print("Preparing data to load")
    patients_json_std_clean = list(map(lambda x: prepare_to_load(x), patients_json_std_telecom))

    return patients_json_std_clean
