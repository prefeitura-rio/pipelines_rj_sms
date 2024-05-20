# -*- coding: utf-8 -*-
import re
from operator import itemgetter

import pandas as pd
from unidecode import unidecode

from pipelines.prontuarios.std.formatters.generic.patient import (
    clean_email_records,
    clean_name_fields,
    clean_phone_records,
    clean_postal_code_info,
    dic_cns_value,
)


def transform_to_ibge_code(
    data: dict, city_dict_res: dict, city_dict_nasc: dict, country_dict: dict
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
    # Dados local de residência:
    regexp_ibgecode = re.compile(r"(?P<municipio>[A-Z ]+) \[IBGE: (?P<city>[0-9]{7})\]")
    re_match = regexp_ibgecode.match(data.get("municipioResidencia", ""))
    data["city_vitacare"] = re_match.group("city") if re_match is not None else ""
    data["city"] = city_dict_res.get(data["city_vitacare"])
    data["state"] = data["city"][0:2] if data["city"] is not None else None
    data["country"] = "010" if data["city"] is not None else None

    # Dados local de nascimento:
    municipioNascimento = data.get("municipioNascimento", "")
    data["birth_city_cod"] = city_dict_nasc.get(municipioNascimento.upper())
    data["birth_state_cod"] = (
        data["birth_city_cod"][0:2] if data["birth_city_cod"] is not None else None
    )
    data["birth_country_cod"] = "010" if data["birth_city_cod"] is not None else None

    return data


def standardize_address_data(
    data: dict, city_dict_res: dict, city_dict_nasc: dict, country_dict: dict
) -> dict:
    """
    Standardize address data field to acceptable API format

    Args:
        data (dict) : Individual data record
        logradouros_dict (dict) : From/to dictionary to name logradouros codes
        city_dict_res (dict): From/to dictionary to code city names (used in address)
        city_dict_nasc (dict): From/to dictionary to code city names (used in birth city)
        state_dict (dict): From/to dictionary to code state names
        country_dict (dict): From/to dictionary to verify country codes

    Returns:
        data (dict) : Individual data record standardized
    """
    data = transform_to_ibge_code(data, city_dict_res, city_dict_nasc, country_dict)

    cep_list = [field for field in data.keys() if field in ["end_cep", "cep", "CEP_LOGRADOURO"]]
    if len(cep_list) == 1:
        cep_field = cep_list[0]
        cep = data[cep_field]
        cep = re.sub("[^0-9]", "", cep)
        cep_l = cep[5:]
        if cep_l != "":
            cep_l_std = cep_l.rjust(3, "0")
            cep = cep[:5] + cep_l_std
            data[cep_field] = cep
            data = clean_postal_code_info(data)

    if pd.isna(data.get("tipoLogradouro")):
        data["tipoLogradouro"] = "Rua"

    address_dic = {
        "use": None,
        "type": None,
        "line": data.get("tipoLogradouro") + " " + data.get("logradouro")
        if (pd.isna(data.get("logradouro")) is False)
        else None,
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

    phone_std = clean_phone_records(data.get("telefone"))
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
    phone_clean_list.append(format_telecom(phone_std, "phone"))
    phone_clean_list.append(format_telecom(data.get("email"), "email"))

    telecom_list = [contact for contact in phone_clean_list if contact is not None]

    if len(telecom_list) > 0:
        data["telecom_list"] = telecom_list
    else:
        pass

    return data


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
        data["racaCor"] = unidecode(data.get("racaCor"))
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
    nationality_dict = {"BRASILEIRA": "B", "ESTRANGEIRO": "E", "NATURALIZADO": "N"}

    if (data.get("nacionalidade") is None) | (data.get("nacionalidade") == ""):
        return data
    elif data.get("nacionalidade", "").upper() in nationality_dict.keys():
        data["nationality"] = nationality_dict[data["nacionalidade"].upper()]
        return data
    else:
        return data


def standardize_parents_names(data: dict) -> dict:
    """
    Standardize parent fields to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data["mother_name"] = (
        clean_name_fields(data.get("nomeMae"))
        if clean_name_fields(data.get("nomeMae")) != ""
        else None
    )
    data["father_name"] = (
        clean_name_fields(data.get("nomePai"))
        if clean_name_fields(data.get("nomePai")) != ""
        else None
    )
    return data


def standardize_cns_list(data: dict) -> dict:
    """
    Standardize cns_list field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    cns = data.get("cns")
    cns_std = dic_cns_value(cns, False)

    if cns_std is not None:
        data["cns_list"] = [cns_std]

    return data


def standardize_decease_info(data: dict) -> dict:
    """
    Standardize decease field to acceptable API values
    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if (data.get("obito") == "false") | (data.get("obito") is False):
        data["deceased"] = False
    if (data.get("obito") == "true") | (data.get("obito") is True):
        data["deceased"] = True
    else:
        data["deceased"] = None

    return data
