# -*- coding: utf-8 -*-
import json
import re
from operator import itemgetter

import pandas as pd

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
    if (data["racaCor"] is None) | (data["nacionalidade"] == ""):
        return data
    elif bool(re.search("SEM INFO", data["racaCor"])):
        return data
    else:
        data["race"] = data["racaCor"].lower()
        return data


def standardize_nationality(data: dict) -> dict:
    """
    Standardize nationality field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if (data["nacionalidade"] is None) | (data["nacionalidade"] == ""):
        return data
    elif data["nacionalidade"] == "A":
        return data
    else:
        data["nationality"] = data["nacionalidade"]
        return data


def standardize_parents_names(data: dict) -> dict:
    """
    Standardize parents names field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data["father_name"] = clean_name_fields(data["nome_pai"])
    data["mother_name"] = clean_name_fields(data["nome_mae"])
    return data


def standardize_cns_list(data: dict) -> dict:
    """
    Standardize cns_list field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    lista = data["cns_provisorio"]
    lista = json.loads(lista)

    if (len(lista) == 1) & (lista[0] is None):
        return data
    elif len(lista) == 1:
        data["cns_list"] = list(map(lambda p: dic_cns_value(p, True), lista))
        return data
    else:
        data["cns_list"] = [dic_cns_value(lista[0], True)] + list(
            map(lambda p: dic_cns_value(p, False), lista[1:])
        )
        return data


def standardize_decease_info(data: dict) -> dict:
    """
    Standardize decease field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data["deceased_date"] = clean_datetime_field(data["dt_obito"])

    if data["obito"] == "1":
        data["deceased"] = True
    elif data["obito"] == "0":
        data["deceased"] = False
    elif pd.notna(data["deceased_date"]):
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
        "use": None,  # nao sei onde tem essa info ainda
        "type": None,  # nao sei onde tem essa info ainda
        "line": format_address("end_tp_logrado_nome", "end_logrado", "end_numero", "end_complem"),
        "city": data["city"],
        "country": data["country"],
        "state": data["state"],
        "postal_code": data["postal_code"],
        "start": None,  # nao sei onde tem essa info ainda
        "end": None,  # nao sei onde tem essa info ainda
    }

    # Testa valores obrigatorios, se algum faltar nao retornamos endereço
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
    data, _ = clean_phone_records(data)
    data = clean_email_records(data)

    def format_telecom(record, type_telecom):
        if (record is None) | (pd.isna(record)):
            return
        else:
            telecom_dic = {
                "system": type_telecom,
                "use": None,  # nao sei onde tem essa info ainda
                "value": record,
                "rank": None,  # nao sei onde tem essa info ainda
                "start": None,  # nao sei onde tem essa info ainda
                "end": None,  # nao sei onde tem essa info ainda
            }

            telecom_dic = dict(filter(lambda item: item[1] is not None, telecom_dic.items()))
            return telecom_dic

    telefone = format_telecom(data["telefone"], "phone")  # nao aceita lista por eqt
    email = format_telecom(data["email"], "email")  # nao aceita lista por eqt

    telecom_list = [contact for contact in [telefone, email] if contact is not None]

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
    if data["end_tp_logrado_cod"] in logradouros_dict.keys():
        data["end_tp_logrado_nome"] = logradouros_dict[data["end_tp_logrado_cod"]]
    else:
        data["end_tp_logrado_nome"] = None

    # Dados local de residencia:
    if (data["cod_mun_res"] in city_dict.keys()) & (data["uf_res"] in state_dict.keys()):
        data["city"] = city_dict[data["cod_mun_res"]]
        data["state"] = state_dict[data["uf_res"]]
        data["country"] = "1"
    elif data["cod_mun_res"] in city_dict.keys():
        data["city"] = city_dict[data["cod_mun_res"]]
        data["state"] = data["city"][0:2]
        data["country"] = "1"
    else:
        data["city"] = None
        data["state"] = None
        data["country"] = None


   
    if (data["cod_mun_nasc"] in city_dict.keys()) & (data["uf_nasc"] in state_dict.keys()) & (data["cod_pais_nasc"] in country_dict.keys()):
        data["birth_city_cod"] = city_dict[data["cod_mun_nasc"]]
        data["birth_state_cod"] = state_dict[data["uf_nasc"]]
        data["birth_country_cod"] = country_dict[data['cod_pais_nasc']]
    elif data["cod_mun_nasc"] in city_dict.keys():
        data["birth_city_cod"] = city_dict[data["cod_mun_nasc"]]
        data["birth_state_cod"] = data["birth_city_code"][0:2]
        data["birth_country_cod"] = "010"
    else:
        pass

    return data
