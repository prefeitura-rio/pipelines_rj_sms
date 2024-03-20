# -*- coding: utf-8 -*-
import re
from operator import itemgetter

import pandas as pd
from unidecode import unidecode
from validate_docbr import CNS, CPF


def merge_keys(dic: dict) -> dict:
    """
    Formating payload flatting json
    Args:
        dic (dict) : Individual data record
    Returns:
        dic (dict) : Individual data record flatted
    """
    subdic = dic.pop("data")
    if "id" in subdic.keys():
        subdic.pop("id")
    dic.update(subdic)

    return dic


def drop_invalid_records(data: dict) -> dict:
    """
    Replace patient records to None if the record does not contain all the API required fiels

    Args:
        dic (dict) : Individual data record
    Returns:
        dic (dict) : Individual data record standardized or None
    """
    data["raw_source_id"] = data["id"]
    birth_date_field = [field for field in data.keys() if field in ["dataNascimento", "dt_nasc"]][0]
    data["birth_date"] = clean_datetime_field(data[birth_date_field])

    # Remove registros com cpf invalido ou nulo
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
    if (data["sexo"] == "1") | (data["sexo"] == "M"):
        data["gender"] = "male"
    elif (data["sexo"] == "2") | (data["sexo"] == "F"):
        data["gender"] = "female"
    else:
        data["gender"] = None

    # Remove registros com nome nulo ou inválido
    data["name"] = clean_name_fields(data["nome"])

    # Drop
    for value in list(itemgetter("patient_cpf", "gender", "birth_date")(data)):
        if (value is None) | (pd.isna(value)):
            return
        else:
            pass
    data["patient_code"] = data["patient_cpf"] + "." + data["birth_date"].replace("-", "")
    return data


def clean_none_records(json_list: list) -> list:
    """
    Deleting None records (invalidated records) from payload
    Args:
        json_list (list): Payload standartized
    Returns:
        list: Payload standartized without None elements
    """
    return [record for record in json_list if record is not None]


def prepare_to_load(data: dict) -> dict:
    """
    Drop fields not contained in API from individual patient record

    Args:
        data (dict) : Individual data record
        lista_campos_api (list) : Fields acceptable in API

    Returns:
        data (dict) : Individual data record standardized
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
        "patient_code",
    ]
    data = dict(
        filter(lambda item: (item[1] is not None) & (item[0] in lista_campos_api), data.items())
    )
    return data


def clean_datetime_field(datetime: str) -> str:
    """
    Normalizing datetime fields
    Args:
        datetime (str): Data field to be cleaned/validated
    Returns:
        str: Normalized and validated data
    """
    if datetime is None:
        return None
    else:
        clean_datetime = re.sub(r" .*", "", datetime)
        clean_datetime = re.sub(r"T.*", "", clean_datetime)
        clean_datetime = pd.to_datetime(clean_datetime, format="%Y-%m-%d", errors="coerce")
        clean_datetime = str(clean_datetime.date()) if pd.isna(clean_datetime) is False else None
        return clean_datetime


def clean_name_fields(name: str) -> str:
    """
    clean name

    Args:
        name (str): Name field to be standardized
    Returns:
        str: Standardized name string
    """
    if name is None:
        return
    else:

        name = re.sub(r"\( *(ALEGAD[O|A]) *\)", "", name)
        name = re.sub(r"[´`'.]", "", name)
        name = re.sub(r"Ã§", "C", name)
        name = re.sub(r'[!"#$%&\'()*+,-\/:;<=>?@[\\\]^_`{|}~]', "", name)
        name = name.replace("\t", "")
        name = name.strip()
        name = re.sub("(SEM INFO)|(DECLAR)|(PAC CHEGOU COM OS BOMBEIROS)", "", name)
        name = unidecode(name)

        if bool(re.search(r"[^\w ]", name)) | len(name.split()) < 2:
            return ""
        else:
            return name


def dic_cns_value(valor: str, is_main: bool) -> dict:
    """
    Format register dictionary of CNS value
    Args:
        valor (str): CNS value
        is_main (bool): Flag if CNS value is main patient CNS
    Returns:
        dict: CNS info dictionary
    """
    cns = CNS()
    valor = re.sub("[^0-9]", "", valor)
    if valor is None:
        return
    elif cns.validate(valor):
        return {"value": valor, "is_main": is_main}  # o primeiro da lista é o main
    else:
        return


def format_address(pp_type: str, pp_name: str, number: str, compl: str) -> str:
    """
    Returns full line address combining columns

    Args:
    pp_type (str): Type of public place
    pp_name (str): Public place name
    number (str):

    Returns:
        str: Full adress line
    """
    logrado_type = pp_type if pp_type is not None else ""
    logrado = pp_name if pp_name is not None else ""
    number = f", {number}" if number is not None else ""
    compl = compl if compl is not None else ""

    return f"{logrado_type} {logrado}{number} {compl}"


def clean_postal_code_info(data: dict) -> dict:
    """
    Validade cep info

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    cep_field = [field for field in data.keys() if field in ['end_cep', 'cep']][0]
    if data[cep_field] is None:
        data["postal_code"] = None
        return data
    else:
        data[cep_field] = re.sub(r"[^0-9]", "", data[cep_field])
        if len(data[cep_field]) != 8:
            data["postal_code"] = None
            return data
        else:
            data["postal_code"] = data[cep_field]
    return data


def clean_phone_records(data: dict) -> dict:
    """
    Standardize phone data field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
        phone_fields (list) : List of phone fields
    """
    phone_fields = [
        column
        for column in data.keys()
        if column in ["telefone", "celular", "telefoneExtraUm", "telefoneExtraDois"]
    ]
    for phone_field in phone_fields:
        if data[phone_field] is not None:
            data[phone_field] = re.sub(r"[()-]", "", data[phone_field])
            if (len(data[phone_field]) < 8) | (len(data[phone_field]) > 12):
                data[phone_field] = None
            elif bool(
                re.search(
                    "0{8,}|1{8,}|2{8,}|3{8,}|4{8,}|5{8,}|6{8,}|7{8,}|8{8,}|9{8,}", data[phone_field]
                )
            ):
                data[phone_field] = None
            elif bool(re.search("[^0-9]", data[phone_field])):
                data[phone_field] = None
    return data, phone_fields


def clean_email_records(data: dict) -> dict:
    """
    Validate email info

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
