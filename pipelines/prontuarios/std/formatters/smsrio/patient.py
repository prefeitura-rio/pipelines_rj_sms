# -*- coding: utf-8 -*-
import json
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


def standardize_race(data: dict) -> dict:
    """
    Standardize race field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if (data.get("racaCor") is None) | (data.get("racaCor") == ""):
        return data
    elif bool(re.search("SEM INFO", data.get("racaCor"))):
        return data
    else:
        data["racaCor"] = data.get("racaCor").lower()
        data["racaCor"] = unidecode(data["racaCor"])
        if data["racaCor"] in ["branca", "preta", "parda", "amarela", "indigena"]:
            data["race"] = data["racaCor"]
        return data


def standardize_nationality(data: dict) -> dict:
    """
    Standardize nationality field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if (data.get("nacionalidade") is None) | (data.get("nacionalidade") == ""):
        return data
    elif data.get("nacionalidade") == "A":
        return data
    else:
        data["nationality"] = data.get("nacionalidade")
        return data


def standardize_parents_names(data: dict) -> dict:
    """
    Standardize parents names field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data["father_name"] = clean_name_fields(data.get("nome_pai"))
    data["mother_name"] = clean_name_fields(data.get("nome_mae"))
    return data


def standardize_cns_list(data: dict) -> dict:
    """
    Standardize cns_list field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    lista = data.get("cns_provisorio")
    lista = json.loads(lista) if lista is not None else [None]

    if (len(lista) == 1) & (lista[0] is None):
        return data
    elif len(lista) == 1:
        data["cns_list"] = list(map(lambda p: dic_cns_value(p, True), lista))
        return data
    else:
        lista.reverse()
        cns_list_std = []
        cns_list_values = []
        for ind, value_raw in enumerate(lista):
            value_std = None
            if ind == 0:
                value_std = dic_cns_value(value_raw, True)
            else:
                if value_raw not in cns_list_values:
                    value_std = dic_cns_value(value_raw, False)

            if value_std is not None:
                cns_list_std.append(value_std)
                cns_list_values.append(value_raw)
        data["cns_list"] = cns_list_std

        return data


def standardize_decease_info(data: dict) -> dict:
    """
    Standardize decease field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data["deceased_date"] = clean_datetime_field(data.get("dt_obito"))

    if data.get("obito") == "1":
        data["deceased"] = True
    elif data.get("obito") == "0":
        data["deceased"] = False
    elif pd.notna(data.get("deceased_date")):
        data["deceased"] = True
    else:
        pass

    return data


def standardize_address_data(
    data: dict, logradouros_dict: dict, city_dict: dict, state_dict: dict, country_dict: dict
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
    data = transform_to_ibge_code(data, logradouros_dict, city_dict, state_dict, country_dict)
    data = clean_postal_code_info(data)

    address_dic = {
        "use": None,
        "type": None,
        "line": format_address(
            data.get("end_tp_logrado_nome"),
            data.get("end_logrado"),
            data.get("end_numero"),
            data.get("end_complem"),
        ),
        "city": data.get("city"),
        "country": data.get("country"),
        "state": data.get("state"),
        "postal_code": data.get("postal_code"),
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

    phones_unique = (
        json.loads(data.get("telefones")) if data.get("telefones") is not None else [None]
    )
    phones_unique = list(set(phones_unique))
    phones_std = list(map(clean_phone_records, phones_unique))
    data = clean_email_records(data)

    def format_telecom(record, type_telecom):
        if (record is None) | (pd.isna(record)):
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

    phone_clean_list = []
    for phone in phones_std:
        if phone is not None:
            phone_clean_list.append(format_telecom(phone, "phone"))

    phone_clean_list.append(format_telecom(data.get("email"), "email"))

    telecom_list = [contact for contact in phone_clean_list if contact is not None]

    if len(telecom_list) > 0:
        data["telecom_list"] = telecom_list
    else:
        pass

    return data


def transform_to_ibge_code(
    data: dict, logradouros_dict: dict, city_dict: dict, state_dict: dict, country_dict: dict
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
    # Normalizando tipo de logradouros
    if data.get("end_tp_logrado_cod") in logradouros_dict.keys():
        data["end_tp_logrado_nome"] = logradouros_dict[data.get("end_tp_logrado_cod")]
    else:
        data["end_tp_logrado_nome"] = None

    # Dados local de residencia:
    if data.get("cod_mun_res") in city_dict.keys():
        data["city"] = city_dict[data.get("cod_mun_res")]
        data["state"] = data["city"][0:2]
        data["country"] = "010"
    else:
        data["city"] = None
        data["state"] = None
        data["country"] = None

    # Dados local de nascimento:
    if data.get("cod_mun_nasc") in city_dict.keys():
        data["birth_city_cod"] = city_dict[data.get("cod_mun_nasc")]
        data["birth_state_cod"] = data["birth_city_cod"][0:2]
        data["birth_country_cod"] = "010"
    else:
        pass

    return data
