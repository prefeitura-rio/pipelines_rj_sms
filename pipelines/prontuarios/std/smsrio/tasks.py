# -*- coding: utf-8 -*-
import json
import re
from operator import itemgetter
from typing import Tuple

import pandas as pd
from prefect import task
from validate_docbr import CNS, CPF

from pipelines.prontuarios.std.smsrio.utils import (
    clean_email_records,
    clean_phone_records,
    clean_postal_code_info,
    format_address
    )


@task
def get_params(start_datetime, end_datetime):
    return {
        'start_datetime': start_datetime,
        'end_datetime': end_datetime,
        'datasource_system': 'smsrio'
    }


@task
def merge_keys(dic: dict) -> dict:
    """
    Formating payload flatting json
    Args:
        dic (dict) : Individual data record
    Returns:
        dic (dict) : Individual data record flatted
    """
    subdic = dic.pop("data")
    dic.update(subdic)

    return dic


def define_constants() -> Tuple[list, dict, dict, dict, dict]:
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
def drop_invalid_records(data: dict) -> dict:
    """
    Replace patient records to None if the record does not contain all the API required fiels

    Args:
        dic (dict) : Individual data record
    Returns:
        dic (dict) : Individual data record standardized or None
    '''
    # Adiciona raw source id
    data['raw_source_id'] = data['id']

    # Remove registros com data nula ou invalida
    data["birth_date"] = re.sub(r"T.*", "", data['dt_nasc'])
    data["birth_date"] = pd.to_datetime(data['birth_date'], format='%Y-%m-%d', errors='coerce')
    if pd.notna(data['birth_date']):
        data['birth_date'] = str(data['birth_date'].date())

    # Remove registros com cpf invalido ou nulo
    # falta melhorar essa validação com cadsus
    cpf = CPF()
    if data["patient_cpf"] is None:
        pass
    elif data["patient_cpf"] == "01234567890":
        data["patient_cpf"] = None
    elif cpf.validate(data["patient_cpf"]) is False:
        data["patient_cpf"] = None
    else:
        pass

    # Remove registros com sexo nulo ou invalido
    if data["sexo"] == "1":
        data["gender"] = "male"
    elif data["sexo"] == "2":
        data["gender"] = "female"
    else:
        data["gender"] = None

    # Remove registros com nome nulo ou inválido
    if bool(re.search(r"[^\w ]", data["nome"])):
        data["name"] = None
    elif len(data["nome"].split()) < 2:
        data["name"] = None
    else:
        data["name"] = data["nome"]

    # Drop
    for value in list(itemgetter("patient_cpf", "name", "gender", "birth_date")(data)):
        if (value is None) | (pd.isna(value)):
            return
        else:
            pass
    return data


@task
def clean_records_fields(data: dict, lista_campos_api: list) -> dict:
    """
    Drop fields not contained in API from individual patient record

    Args:
        data (dict) : Individual data record
        lista_campos_api (list) : Fields acceptable in API

    Returns:
        data (dict) : Individual data record standardized
    """
    data = dict(
        filter(lambda item: (item[1] is not None) & (item[0] in lista_campos_api), data.items())
    )
    return data


@task
def clean_none_records(json_list: list) -> list:
    """
    Deleting None records (invalidated records) from payload
    Args:
        json_list (list): Payload standartized
    Returns:
        list: Payload standartized without None elements
    """
    return [record for record in json_list if record is not None]


@task
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
    elif bool(re.search("SEM INFO", data["racaCor"])):
        return data
    else:
        data["race"] = data["racaCor"].lower()
        return data


@task
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
    elif data["nacionalidade"] == "A":
        return data
    else:
        data["nationality"] = data["nacionalidade"]
        return data


@task
def standardize_parents_names(data: dict) -> dict:
    """
    Standardize parents names field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    # Nome do pai
    if data["nome_pai"] is None:
        pass
    elif (len(data["nome_pai"].split()) < 2) | (bool(re.search("SEM INFO", data["nome_pai"]))):
        pass
    else:
        data["father_name"] = data["nome_pai"]

    # Nome da mãe
    if data["nome_mae"] is None:
        pass
    elif (len(data["nome_mae"].split()) < 2) | (bool(re.search("SEM INFO", data["nome_mae"]))):
        pass
    else:
        data["mother_name"] = data["nome_mae"]

    return data


@task
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

    def dic_cns_value(valor, is_main):
        cns = CNS()
        if valor is None:
            return
        elif cns.validate(valor):
            return {"value": valor, "is_main": is_main}  # o primeiro da lista é o main
        else:
            return

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


@task
def standardize_decease_info(data: dict) -> dict:
    """
    Standardize decease field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    '''
    if data['dt_obito'] is not None:
        data["deceased_date"] = re.sub(r"T.*", "", data['dt_obito'])
        data['deceased_date'] = pd.to_datetime(data['dt_obito'], format='%Y-%m-%d', errors='coerce')
        if pd.notna(data['deceased_date']):
            data['deceased_date'] = str(data['deceased_date'].date())
        else:
            data['deceased_date'] = None
    else:
        data["deceased_date"] = None

    if (data['obito'] == '1'):
        data['deceased'] = True
    elif (data['obito'] == '0'):
        data['deceased'] = False
    elif (pd.notna(data['deceased_date'])):
        data['deceased'] = True
    else:
        pass

    return data


@task
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
        "line": format_address(data),
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


@task
def standardize_telecom_data(data: dict) -> dict:
    """
    Standardize telecom data field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    data = clean_phone_records(data)
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
