# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Tasks for execute_dbt
"""

import json
import re
from datetime import timedelta

import pandas as pd
import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.transform.gemini.pacientes_restritos.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task


@task(nout=3)
def reduce_raw_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduce the column 'motivo_atendimento' to only the strings that contain the keywords
    Args:
        df (pd.DataFrame): Dataframe with the column 'motivo_atendimento' to be reduced
    Returns:
        df (pd.DataFrame): Dataframe with the column 'motivo_atendimento' reduced
    """

    def reduce_raw_text_row(raw):
        raw = raw.lower()
        list_strings = raw.split(".")
        pattern = r"(hiv positivo)|(hiv+)|(soropositivo)|(aids)|(imunodefici[ê|e]ncia humana)|(aid[é|e]tico)"
        list_strings_stigma = [item for item in list_strings if bool(re.search(pattern, item))]
        return ".".join(list_strings_stigma)

    df["motivo_atendimento_reduced"] = df["clinical_motivation"].apply(
        lambda x: reduce_raw_text_row(x)
    )

    return df["motivo_atendimento_reduced"].values, df["id_hci"].values, df["cpf"].values


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_result_gemini(atendimento: str, id_atendimento: str, cpf: str, gemini_key=str) -> dict:
    """
    Get the result from Gemini
    Args:
        atendimento (str): Atendimento to be analyzed
        id_atendimento (str): ID of the atendimento
        cpf (str): CPF of the patient
        gemini_key (str): Key to access Gemini
    Returns:
        result (dict): Result from Gemini
    """
    prompt = """
    Abaixo um relato clinico relacionado a um paciente quem contém alguma menção ao diagnóstico de HIV.
    Identifique com o formato {"flag": "1 caso o paciente em questao tenha o diagnostico confirmado e 0 caso contrário.", "motivo":"Explique o motivo de escolha da flag"}
    """
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{constants.GEMINI_MODEL.value}:generateContent?key={gemini_key}"

    data = {"contents": [{"parts": [{"text": prompt + atendimento}]}]}
    headers = {"Content-type": "application/json"}

    response = requests.post(url, data=json.dumps(data), headers=headers)

    if response.status_code == 200:
        response = response.json()["candidates"][0]["content"]["parts"][0]["text"]
        result = {"result": response, "raw": atendimento, "id_hci": id_atendimento, "cpf": cpf}

        return result
    else:
        raise ValueError(f"API call failed, error: {response.status_code} - {response.reason}")


@task
def parse_result_dataframe(list_result: list) -> pd.DataFrame:
    """
    Parse the result from Gemini
    Args:
        list_result (list): List of results from Gemini
    Returns:
        df (pd.DataFrame): Dataframe with the results
    """

    def parse_result_row(dict_result: str) -> dict:
        """
        Parse the result from Gemini
        Args:
            dict_result (str): Result from Gemini
        Returns:
            result (dict): Result from Gemini
        """
        dict_result = dict_result.replace("\n", "")
        regexp_flag = re.compile(r'.*{"flag": {0,1}(?P<flag>.*), "motivo": {0,1}(?P<motivo>.*)}.*')
        re_match = regexp_flag.match(dict_result)

        if re_match is None:
            return {"flag": "", "motivo": dict_result}

        if "flag" in re_match.groupdict().keys():
            flag = re_match.group("flag")
            flag = flag.replace('"', "")
            flag = flag.replace('"', "")
        else:
            flag = ""

        if "motivo" in re_match.groupdict().keys():
            motivo = re_match.group("motivo")
            motivo = motivo.replace('"', "")
            motivo = motivo.replace('"', "")
        else:
            motivo = dict_result

        result = {"flag": flag, "motivo": motivo}

        return result

    df = pd.DataFrame(list_result)
    df["result_json"] = df["result"].apply(lambda x: parse_result_row(x))
    df = pd.json_normalize(
        df[["raw", "id_hci", "cpf", "result_json"]].to_dict(orient="records"), max_level=1
    )
    df.rename(
        {"result_json.flag": "flag_gemini", "result_json.motivo": "motivo_gemini"},
        axis=1,
        inplace=True,
    )
    df["_extracted_at"] = pd.Timestamp.now()

    return df
