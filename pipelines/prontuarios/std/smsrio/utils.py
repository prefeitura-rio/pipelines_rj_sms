# -*- coding: utf-8 -*-
import re


def clean_email_records(data: dict) -> dict:

    """
    Standardize email data field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data["email"] is not None:
        data["email"] = re.sub(r" ", "", data["email"])
        if not bool(re.search(r"^[\w\-\.]+@([\w\-]+\.)+[\w\-]{2,4}$", data["email"])):
            data["email"] = None
        else:
            pass
    else:
        pass
    return data


def clean_phone_records(data: dict) -> dict:

    """
    Standardize phone data field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data["telefone"] is not None:
        data["telefone"] = re.sub(r"[()-]", "", data["telefone"])
        if (len(data["telefone"]) < 8) | (len(data["telefone"]) > 12):
            # tel fixos tem 8 digitos e contando com DDD sao 12 digitos
            # mas nao validamos DDD nem vimos se telefone celular tem 9 dig
            data["telefone"] = None
        elif bool(
            re.search(
                "0{8,}|1{8,}|2{8,}|3{8,}|4{8,}|5{8,}|6{8,}|7{8,}|8{8,}|9{8,}", data["telefone"]
            )
        ):
            data["telefone"] = None
        elif bool(re.search("[^0-9]", data["telefone"])):
            data["telefone"] = None
    return data


def clean_postal_code_info(data: dict) -> dict:

    """
    Standardize postal code data field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data["end_cep"] is None:
        data["postal_code"] = None
        return data
    else:
        data["end_cep"] = re.sub(r"[^0-9]", "", data["end_cep"])
        if len(data["end_cep"]) != 8:
            data["postal_code"] = None
            return data
        else:
            data["postal_code"] = data["end_cep"]  # [0:5] + '-' data['end_cep'][5:9]
    return data


def format_address(row: dict) -> str:

    """
    Returns full address combining columns

    Args:
    row (pandas.Series): Row from raw patient data

    Returns:
        str: Full adress line
    """
    logrado_type = row["end_tp_logrado_nome"] if row["end_tp_logrado_nome"] is not None else ""
    logrado = row["end_logrado"] if row["end_logrado"] is not None else ""
    number = f", {row['end_numero']}" if row["end_numero"] is not None else ""
    compl = row["end_complem"] if row["end_complem"] is not None else ""

    return f"{logrado_type} {logrado}{number} {compl}"


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
    elif data["cod_mun_res"] in city_dict.keys():
        data["city"] = city_dict[data["cod_mun_res"]]
        data["state"] = data["city"][0:2]
    else:
        data["city"] = None
        data["state"] = None

    # Dados local de nascimento
    if (data["cod_mun_nasc"] in city_dict.keys()) & (data["uf_nasc"] in state_dict.keys()):
        data["birth_city_cod"] = city_dict[data["cod_mun_nasc"]]
        data["birth_state_cod"] = state_dict[data["uf_nasc"]]
        data["birth_country_cod"] = "1"
    elif data["cod_mun_nasc"] in city_dict.keys():
        data["birth_city_cod"] = city_dict[data["cod_mun_nasc"]]
        data["birth_state_cod"] = data["birth_city_code"][0:2]
        data["birth_country_cod"] = "1"
    else:
        pass

    # Normalizando pais para código IBGE
    data["country"] = "1"  # n achei info
    # if data['cod_pais_nasc'] in country_dict.keys():
    #     data['birth_country_cod'] = country_dict[data['cod_pais_nasc']]
    # else:
    #     data['birth_country_cod'] = None

    return data
