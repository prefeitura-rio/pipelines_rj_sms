# -*- coding: utf-8 -*-
import re
from operator import itemgetter

import pandas as pd
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
    data['raw_source_id'] = data['id']

    if data['dataNascimento'] is None:
        data['birth_date'] = None
    else:
        data["birth_date"] = re.sub(r"T.*", "", data['dataNascimento'])
        data["birth_date"] = re.sub(r" .*", "", data['birth_date'])
        data["birth_date"] = pd.to_datetime(data['birth_date'], format='%Y-%m-%d', errors='coerce')
        if not pd.isna(data['birth_date']):
            data['birth_date'] = str(data['birth_date'].date())

    # Remove registros com cpf invalido ou nulo
    # falta validação retirando filhos com cpf dos pais  e cpfs nao correspondentes as pessoas
    cpf = CPF()
    if data["patient_cpf"] is None:
        pass
    elif data['patient_cpf'] == '01234567890':
        data['patient_cpf'] = None
    elif not cpf.validate(data['patient_cpf']):
        data['patient_cpf'] = None
    else:
        pass

    # Remove registros com sexo nulo ou invalido
    if (data["sexo"] == "1") | (data["sexo"] == "M"):
        data["gender"] = "male"
    elif (data["sexo"] == "2") | (data["sexo"] == "F"):
        data["gender"] = "female"
    else:
        data['gender'] = None

    # Remove registros com nome nulo ou inválido
    if data['nome'] is None:
        data['name'] = None
    else:
        data["name"] = re.sub(r"\( *(ALEGAD[O|A]) *\)", "", data['nome'])
        data["name"] = re.sub(r"[´`'.]", "", data['name'])
        data["name"] = re.sub(r"Ã§", "C", data['name'])

        if bool(re.search(r'[^\w ]', data['name'])):
            data['name'] = None
        elif len(data['name'].split()) < 2:
            data['name'] = None
        else:
            data['name'] = data['nome']

    # Drop
    for value in list(itemgetter('patient_cpf', 'name', 'gender', 'birth_date')(data)):
        if (value is None) | (pd.isna(value)):
            return
            return
        else:
            pass
    return data


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


def clean_none_records(json_list: list) -> list:
    """
    Deleting None records (invalidated records) from payload
    Args:
        json_list (list): Payload standartized
    Returns:
        list: Payload standartized without None elements
    """
    return [record for record in json_list if record is not None]


def standardize_race(data):
    """
    Standardize race field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data["racaCor"] is None:
        return data
    elif (bool(re.search('SEM INFO', data['racaCor']))) | \
         (bool(re.search('RECEM NASCIDO N/A', data['racaCor']))):
        return data

    elif data['racaCor'] == 'NEGRO':
        data['race'] = 'preta'
    else:
        data["race"] = data["racaCor"].lower()
    return data


def standardize_nationality(data):
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
        data['nacionality'] = 'E'
    return data


def standardize_parents_names(data):
    """
    Standardize parents names field to acceptable API values

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    # Nome do pai
    if data["nomePai"] is None:
        pass
    else:
        data['nomePai'] = re.sub(r'[!"#$%&\'()*+,-\/:;<=>?@[\\\]^_`{|}~]', "", data['nomePai'])
        data['nomePai'] = data['nomePai'].replace('\t', '')
        data['nomePai'] = data['nomePai'].strip()
        # data['nomePai'] = unidecode(data['nomePai'])
        if (len(data['nomePai'].split()) < 2) | \
           (bool(re.search('(SEM INFO)|(DECLAR)|(PAC CHEGOU COM OS BOMBEIROS)', data['nomePai']))):
            pass
        else:
            data['father_name'] = data['nomePai']

    # Nome da mãe
    if data["nomeMae"] is None:
        pass
    else:
        data['nomeMae'] = re.sub(r'[!"#$%&\'()*+,-\/:;<=>?@[\\\]^_`{|}~]', "", data['nomeMae'])
        data['nomeMae'] = data['nomeMae'].replace('\t', '')
        data['nomeMae'] = data['nomeMae'].strip()
        # data['nomeMae'] = unidecode(data['nomeMae'])
        if (len(data['nomeMae'].split()) < 2) | \
           (bool(re.search('(SEM INFO)|(DECLAR)|(PAC CHEGOU COM OS BOMBEIROS)', data['nomeMae']))):
            pass
        else:
            data['mother_name'] = data['nomeMae']

    return data


def standardize_cns_list(data):
    lista = [data['cns']]
    # lista = json.loads(lista)
    if (len(lista) == 1) & ((lista[0] is None) | (lista[0] == '')):
        return data
    elif len(lista) == 1:
        cns_list = [cns for cns in list(map(lambda p: dic_cns_value(p, True), lista))
                    if cns is not None]
        if len(cns_list) == 1:
            data['cns_list'] = list(map(lambda p: dic_cns_value(p, True), lista))
        else:
            pass
        return data
    else:
        cns_list = [dic_cns_value(lista[0], True)]\
                    + list(map(lambda p: dic_cns_value(p, False), lista[1:]))
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
    if data['dataObito'] is not None:
        data["deceased_date"] = re.sub(r" .*", "", data['dataObito'])
        data['deceased_date'] = pd.to_datetime(data['deceased_date'],
                                               format='%Y-%m-%d',
                                               errors='coerce')
        if not pd.isna(data['deceased_date']):
            data['deceased_date'] = str(data['deceased_date'].date())
            data['deceased'] = True
        else:
            data["deceased_date"] = None
            data["deceased"] = False
    else:
        data['deceased_date'] = None
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
                    'use': None,  # nao sei onde tem essa info ainda
                    'type': None,  # nao sei onde tem essa info ainda
                    'line': format_address(data),
                    'city': data['city'],
                    'country': data['country'],
                    'state': data['state'],
                    'postal_code': None,  # data['postal_code'], # nao temos na VITAI
                    'start': None,  # nao sei onde tem essa info ainda
                    'end': None,  # nao sei onde tem essa info ainda
    }

    # Testa valores obrigatorios, se algum faltar nao retornamos endereço
    for value in list(itemgetter('line', 'city', 'country', 'state')(address_dic)):
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
    data = clean_phone_records(data)
    # data = clean_email_records(data)

    def format_telecom(record, type_telecom):
        if (record is None) | (pd.isna(record)):
            return
        else:
            telecom_dic = {
                "system": type_telecom,
                "use": None,  # nao sei onde tem essa info ainda
                "use": None,  # nao sei onde tem essa info ainda
                "value": record,
                "rank": None,  # nao sei onde tem essa info ainda
                "start": None,  # nao sei onde tem essa info ainda
                "end": None  # nao sei onde tem essa info ainda
              }
            telecom_dic = dict(filter(lambda item: item[1] is not None, telecom_dic.items()))
            return telecom_dic

    telefone = format_telecom(data['telefone'], 'phone')  # nao aceita lista por eqt
    # email = format_telecom(data['email'],'email') # nao aceita lista por eqt # NAO TEM NA VITAI

    telecom_list = [contact for contact in [telefone] if contact is not None]

    if len(telecom_list) > 0:
        data['telecom_list'] = telecom_list
    else:
        pass
    return data


def dic_cns_value(valor, is_main):
    cns = CNS()
    valor = valor.replace('\t', '')
    if valor is None:
        return
    elif cns.validate(valor):
        return {"value": valor, "is_main": is_main}  # o primeiro da lista é o main
    else:
        return


def transform_to_ibge_code(data: dict,
                           city_name_dict: dict,
                           state_dict: dict,
                           country_dict: dict) -> dict:

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
    if (data['municipio'] in city_name_dict.keys()) & (data['uf'] in state_dict.keys()):
        data['city'] = city_name_dict[data['municipio']]
        data['state'] = state_dict[data['uf']]
    elif data['municipio'] in city_name_dict.keys():
        data['city'] = city_name_dict[data['municipio']]
        data['state'] = data['city'][0:2]
    else:
        data['city'] = None
        data['state'] = None

    # Normalizando pais para código IBGE
    data['country'] = '1'  # n achei info
    # if data['cod_pais_nasc'] in country_dict.keys():
    #     data['birth_country_cod'] = country_dict[data['cod_pais_nasc']]
    # else:
    #     data['birth_country_cod'] = None

    return data


def format_address(row) -> str:
    """
    Returns full address combining columns

    Args:
    row (pandas.Series): Row from raw patient data

    Returns:
        str: Full adress line
    """
    logrado_type = row["tipoLogradouro"] if row["tipoLogradouro"] is not None else ""
    logrado = row["nomeLogradouro"] if row["nomeLogradouro"] is not None else ""
    number = f", {row['numero']}" if row["numero"] is not None else ""
    compl = row["complemento"] if row["complemento"] is not None else ""

    return f"{logrado_type} {logrado}{number} {compl}"


def clean_phone_records(data: dict) -> dict:

    """
    Standardize phone data field to acceptable API format

    Args:
        data (dict) : Individual data record

    Returns:
        data (dict) : Individual data record standardized
    """
    if data['telefone'] is not None:
        data['telefone'] = re.sub(r"[()-]", "", data['telefone'])
        if (len(data['telefone']) < 8) | (len(data['telefone']) > 12):
            # tel fixos tem 8 digitos e contando com DDD sao 12 digitos,
            #  nao validamos DDD nem vimos se telefone celular tem 9 dig
            data['telefone'] = None
        elif bool(re.search('0{8,}|1{8,}|2{8,}|3{8,}|4{8,}|5{8,}|6{8,}|7{8,}|8{8,}|9{8,}',
                            data['telefone'])):
            data['telefone'] = None
        elif bool(re.search('[^0-9]', data['telefone'])):
            data['telefone'] = None
    return data
