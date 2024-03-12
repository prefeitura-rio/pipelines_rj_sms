# -*- coding: utf-8 -*-
import re
from operator import itemgetter

import pandas as pd

from pipelines.prontuarios.std.formatters.generic.patient import (
    dic_cns_value,
    format_address,
    clean_phone_records,
    clean_email_records,
    clean_name_fields,
    clean_datetime_field,
)


def standardize_parents_names(data: dict) -> dict:
    """
    Standardize parent fields to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data['father_name'] = clean_name_fields(data['nomePai'])
    data['mother_name'] = clean_name_fields(data['nomeMae'])
    return data


def standardize_race(data: dict) -> dict:
    """
    Standardize race field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data["racaCor"] is None:
        return data
    elif (bool(re.search("SEM INFO", data["racaCor"]))) | (
        bool(re.search("RECEM NASCIDO N/A", data["racaCor"]))
    ):
        return data

    elif data["racaCor"] == "NEGRO":
        data["race"] = "preta"
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
    if data["nacionalidade"] is None:
        return data
    elif data["nacionalidade"] == "BRASILEIRA":
        data["nationality"] = "B"
    elif data["nacionalidade"] == "NATURALIZADA":
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
    lista = [data["cns"]]
    if (len(lista) == 1) & ((lista[0] is None) | (lista[0] == "")):
        return data
    elif len(lista) == 1:
        cns_list = [
            cns for cns in list(map(lambda p: dic_cns_value(p, True), lista)) if cns is not None
        ]
        if len(cns_list) == 1:
            data["cns_list"] = list(map(lambda p: dic_cns_value(p, True), lista))
        else:
            pass
        return data
    else:
        cns_list = [dic_cns_value(lista[0], True)] + list(
            map(lambda p: dic_cns_value(p, False), lista[1:])
        )
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
    deceased_date_field = [field for field in data.keys() if field in ['dt_obito', 'dataObito']][0]

    data['deceased_date'] = clean_datetime_field(data[deceased_date_field])
    if not pd.isna(data['deceased_date']):
        data['deceased'] = True
    elif 'obito' in data.keys():
        if data['obito'] == 1:
            data['deceased'] = True
    else:
        data['deceased'] = False

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
    # data = clean_postal_code_info(data)

    address_dic = {
        "use": None,  # nao sei onde tem essa info ainda
        "type": None,  # nao sei onde tem essa info ainda
        "line": format_address(data['tipoLogradouro'],
                               data['nomeLogradouro'],
                               data['numero'],
                               data['complemento']
                               ),
        "city": data["city"],
        "country": data["country"],
        "state": data["state"],
        "postal_code": None,  # data['postal_code'], # nao temos na VITAI
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
    data, phone_field_list = clean_phone_records(data)
    data = clean_email_records(data)

    def format_telecom(record, type_telecom):
        if (record is None) | (pd.isna(record)) | (record == ''):
            return
        else:
            telecom_dic = {
                "system": type_telecom,
                "use": None,  # nao sei onde tem essa info ainda
                "value": record,
                "rank": None,  # nao sei onde tem essa info ainda
                "start": None,  # nao sei onde tem essa info ainda
                "end": None  # nao sei onde tem essa info ainda
            }

            telecom_dic = dict(filter(lambda item: item[1] is not None, telecom_dic.items()))
            return telecom_dic

    phone_list = []
    for phone in phone_field_list:
        telefone = format_telecom(data[phone], 'phone')
        if telefone is not None:
            phone_list.append(telefone)

    email = format_telecom(data['email'], 'email')
    if email is not None:
        phone_list.append(email)

    telecom_list = phone_list

    if len(telecom_list) > 0:
        data['telecom_list'] = telecom_list
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

    # Dados local de residencia (VITAI so tem uma info, consideramos smp residencia?):
    if (data["municipio"] in city_name_dict.keys()) & (data["uf"] in state_dict.keys()):
        data["city"] = city_name_dict[data["municipio"]]
        data["state"] = state_dict[data["uf"]]
    elif data["municipio"] in city_name_dict.keys():
        data["city"] = city_name_dict[data["municipio"]]
        data["state"] = data["city"][0:2]
    else:
        data["city"] = None
        data["state"] = None

    # Normalizando pais para código IBGE
    data["country"] = "1"  # n achei info
    # if data['cod_pais_nasc'] in country_dict.keys():
    #     data['birth_country_cod'] = country_dict[data['cod_pais_nasc']]
    # else:
    #     data['birth_country_cod'] = None

    return data
