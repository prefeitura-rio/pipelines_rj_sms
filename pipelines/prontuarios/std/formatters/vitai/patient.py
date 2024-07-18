# -*- coding: utf-8 -*-
import re
from operator import itemgetter

import pandas as pd
from unidecode import unidecode

from pipelines.prontuarios.std.formatters.generic.patient import (
    clean_datetime_field,
    clean_email_records,
    clean_name_fields,
    clean_phone_records,
    clean_postal_code_info,
    dic_cns_value,
    format_address,
)


def standardize_parents_names(data: dict) -> dict:
    """
    Standardize parent fields to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data["father_name"] = (
        clean_name_fields(data.get("nomePai"))
        if clean_name_fields(data.get("nomePai")) != ""
        else None
    )
    data["mother_name"] = (
        clean_name_fields(data.get("nomeMae"))
        if clean_name_fields(data.get("nomeMae")) != ""
        else None
    )

    return data


def standardize_race(data: dict) -> dict:
    """
    Standardize race field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data.get("racaCor") is None:
        return data
    elif (bool(re.search("SEM INFO", data.get("racaCor")))) | (
        bool(re.search("RECEM NASCIDO N/A", data.get("racaCor")))
    ):
        return data

    elif data.get("racaCor") == "NEGRO":
        data["race"] = "preta"
        return data
    else:
        data["racaCor"] = data.get("racaCor").lower()
        data["racaCor"] = unidecode(data.get("racaCor"))
        if data.get("racaCor") in ["branca", "preta", "parda", "amarela", "indigena"]:
            data["race"] = data.get("racaCor")
        return data


def standardize_nationality(data: dict) -> dict:
    """
    Standardize nationality field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data.get("nacionalidade") is None:
        return data
    elif data.get("nacionalidade") == "BRASILEIRA":
        data["nationality"] = "B"
    elif data.get("nacionalidade") == "NATURALIZADA":
        data["nationality"] = "N"
    else:
        data["nacionality"] = "E"
    return data


def standardize_cns_list(data: dict) -> dict:
    """
    Standardize race field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    lista = [data.get("cns")]
    if (len(lista) == 1) & ((lista[0] is None) | (lista[0] == "")):
        return data
    else:
        cns_list = list(map(lambda p: dic_cns_value(p, False), lista))
        cns_list_clean = [cns for cns in cns_list if cns is not None]
        data["cns_list"] = cns_list_clean
        return data


def standardize_decease_info(data: dict) -> dict:
    """
    Standardize decease field to acceptable API values
    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    deceased_date_list = [field for field in data.keys() if field in ["dt_obito", "dataObito"]]
    if len(deceased_date_list) == 1:
        deceased_date_field = deceased_date_list[0]
        data["deceased_date"] = clean_datetime_field(data[deceased_date_field])
        if not pd.isna(data["deceased_date"]):
            data["deceased"] = True
        elif "obito" in data.keys():
            if data["obito"] == 1:
                data["deceased"] = True
        else:
            data["deceased"] = False

    return data


def standardize_address_data(
    data: dict, city_name_dict: dict, state_dict: dict, country_dict: dict
) -> dict:
    """
    Standardize address data field to acceptable API format

    Args:
        data (dict) : Individual data record
        logradouros_dict (dict) : From/to dictionary to name logradouros codes
        city_dict (dict): From/to dictionary to code city names
        state_dict (dict): From/to dictionary to code state names
        country_dict (dict): From/to dictionary to verify country codes

    Returns:
        data (dict) : Individual data record standardized
    """
    data = transform_to_ibge_code(data, city_name_dict, state_dict, country_dict)
    if "cep" in data.keys():
        data = clean_postal_code_info(data)
        hasCep = 1
    else:
        hasCep = 0

    if pd.isna(data.get("tipoLogradouro")):
        data["tipoLogradouro"] = "Rua"

    address_dic = {
        "use": None,
        "type": None,
        "line": format_address(
            data.get("tipoLogradouro"),
            data.get("nomeLogradouro"),
            data.get("numero"),
            data.get("complemento"),
        ),
        "city": data.get("city"),
        "country": data.get("country"),
        "state": data.get("state"),
        "postal_code": data.get("postal_code") if hasCep == 1 else None,
        "start": None,
        "end": None,
    }

    for value in list(itemgetter("line", "city", "country", "state")(address_dic)):
        if (value is None) | (pd.isna(value)):
            return data
        else:
            pass
    data["address_list"] = [dict(filter(lambda item: item[1] is not None, address_dic.items()))]
    return data


def standardize_telecom_data(data: dict) -> dict:
    """
    Standardize telecom data field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """

    def format_telecom(record, type_telecom):
        if (record is None) | (pd.isna(record)) | (record == ""):
            return
        else:
            telecom_dic = {
                "system": type_telecom,
                "use": None,
                "value": record,
                "rank": None,
                "start": None,
                "end": None,
            }

            telecom_dic = dict(filter(lambda item: item[1] is not None, telecom_dic.items()))
            return telecom_dic

    phone_list = []
    phone_fields = [
        column
        for column in data.keys()
        if column in ["telefone", "celular", "telefoneExtraUm", "telefoneExtraDois"]
    ]
    for phone in phone_fields:
        value_phone_std = clean_phone_records(data[phone])
        telefone = format_telecom(value_phone_std, "phone")
        if telefone is not None:
            phone_list.append(telefone)

    if "email" in data.keys():
        data = clean_email_records(data)
        email = format_telecom(data["email"], "email")
        if email is not None:
            phone_list.append(email)

    telecom_list = phone_list

    if len(telecom_list) > 0:
        data["telecom_list"] = telecom_list
    else:
        pass

    return data


def transform_to_ibge_code(
    data: dict, city_name_dict: dict, state_dict: dict, country_dict: dict
) -> dict:
    """
    Coding fields to IBGE codes

    Args:
        data (dict) : Individual data record
        logradouros_dict (dict) : From/to dictionary to name logradouros codes
        city_dict (dict): From/to dictionary to code city names
        state_dict (dict): From/to dictionary to code state names
        country_dict (dict): From/to dictionary to verify country codes

    Returns:
        data (dict) : Individual data record standardized
    """

    # VITAI so tem uma info, consideramos smp residencia
    municipio = data.get("municipio")
    municipio_std = municipio.upper() if municipio is not None else None
    if municipio_std in city_name_dict.keys():
        data["city"] = city_name_dict[municipio_std]
        data["state"] = data["city"][0:2]
        data["country"] = "010"
    else:
        data["city"] = None
        data["state"] = None
        data["country"] = None

    return data
