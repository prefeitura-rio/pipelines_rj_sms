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
        dic (dict) : Individual data record standardized with is_valid flag
    """
    data["raw_source_id"] = data["id"]
    name_list = [field for field in data.keys() if field in ["nome", "NOME_DA_PESSOA_CADASTRADA"]]
    gender_list = [field for field in data.keys() if field in ["sexo", "SEXO"]]
    birth_date_list = [
        field for field in data.keys() if field in ["dataNascimento", "dt_nasc", "dataNascPaciente"]
    ]

    name_field = name_list[0] if len(name_list) == 1 else ""
    gender_field = gender_list[0] if len(gender_list) == 1 else ""
    birth_date_field = birth_date_list[0] if len(birth_date_list) == 1 else ""

    data["birth_date"] = clean_datetime_field(data.get(birth_date_field))

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
    if (
        (data.get(gender_field) == "1")
        | (data.get(gender_field) == "M")
        | (data.get(gender_field).lower() == "male")
    ):
        data["gender"] = "male"
    elif (
        (data.get(gender_field) == "2")
        | (data.get(gender_field) == "F")
        | (data.get(gender_field).lower() == "female")
    ):
        data["gender"] = "female"
    else:
        data["gender"] = None

    # Remove registros com nome nulo ou inválido
    data["name"] = clean_name_fields(data.get(name_field))

    # Drop
    data["is_valid"] = 1
    for value in list(itemgetter("patient_cpf", "gender", "birth_date")(data)):
        if (value is None) | (pd.isna(value)):
            data["is_valid"] = 0

    if data["is_valid"] == 1:
        data["patient_code"] = data["patient_cpf"] + "." + data["birth_date"].replace("-", "")
    return data


def clean_none_records(json_list: list) -> list:
    """
    Deleting None records (invalidated records) from payload
    Args:
        json_list (list): Payload standartized with is_valid flag
    Returns:
        valid: List of standardized payloads with is_valid flag set to 1
        not_valid: id list of not valid payloads
    """
    valid = [record for record in json_list if record["is_valid"] == 1]
    not_valid = [record["id"] for record in json_list if record["is_valid"] == 0]
    return valid, not_valid


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
        name = name.upper()
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
    valor = re.sub("[^0-9]", "", valor) if valor is not None else None
    if valor is None:
        return
    elif cns.validate(valor):
        return {"value": valor, "is_main": is_main}
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
    cep_list = [field for field in data.keys() if field in ["end_cep", "cep", "CEP_LOGRADOURO"]]
    if len(cep_list) == 1:
        cep_field = cep_list[0]
        if data[cep_field] is not None:
            data[cep_field] = re.sub(r"[^0-9]", "", data[cep_field])
            if len(data[cep_field]) == 8:
                data["postal_code"] = data[cep_field]
    return data


def clean_phone_records(phone_raw: str) -> str:
    """
    Clean and validate phone field

    Args:
        data (str) : Phone value

    Returns:
        phone_std (str) : Valid phone value or None
    """
    if phone_raw is not None:
        phone_std = re.sub(r"[()-]", "", phone_raw)
        if (len(phone_std) < 8) | (len(phone_std) > 12):
            return
        elif bool(
            re.search("0{8,}|1{8,}|2{8,}|3{8,}|4{8,}|5{8,}|6{8,}|7{8,}|8{8,}|9{8,}", phone_std)
        ):
            return
        elif bool(re.search("[^0-9]", phone_std)):
            return
        return phone_std


def clean_email_records(data: dict) -> dict:
    """
    Validate email info

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    email_list = [field for field in data.keys() if field in ["email", "EMAIL_CONTATO"]]
    if len(email_list) == 1:
        email_field = email_list[0]
        if data[email_field] is not None:
            data["email"] = re.sub(r" ", "", data[email_field])
            if not bool(re.search(r"^[\w\-\.]+@([\w\-]+\.)+[\w\-]{2,4}$", data[email_field])):
                data["email"] = None
            else:
                pass
    return data
