# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Tasks for execute_dbt
"""

import re
import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.prontuarios.std.deteccao_restricoes.constants import (
    constants
)
from pipelines.utils.credential_injector import authenticated_task as task
from google import genai
from pipelines.utils.tasks import get_secret_key


def reduce_raw_column(df):
    def reduce_raw_text_row(raw):
        raw = raw.lower()
        list_strings = raw.split('.')
        pattern = r'(hiv positivo)|(hiv+)|(soropositivo)|(aids)|(imunodefici[ê|e]ncia humana)|(aid[é|e]tico)'
        list_strings_stigma = [item for item in list_strings if bool(re.search(pattern,item))]
        return '.'.join(list_strings_stigma)
    df['motivo_atendimento_reduced'] = df['motivo_atendimento'].apply(lambda x: reduce_raw_text_row(x))
    # retirar linhas abaixo
    df['len_motivo_atendimento'] = df['motivo_atendimento'].apply(lambda x: len(x))
    df['len_motivo_atendimento_reduced'] = df['motivo_atendimento_reduced'].apply(lambda x: len(x))
    
    return df


@task
def get_result_gemini(atendimento, flag_rastreio, env):
    def load_gemini_client(env): 
        key = get_secret_key(
            secret_path=constants.INFISICAL_PATH, 
            secret_name=constants.INFISICAL_API_KEY, 
            environment=env) # Ver secret

        client = genai.Client(api_key=key)
        return client
    
    prompt = """
    Abaixo um relato clinico relacionado a um paciente quem contém alguma menção ao diagnóstico de HIV. 
    Identifique com o formato {"flag": "1 caso o paciente em questao tenha o diagnostico e 0 caso contrário.", "motivo":"Explique o motivo de escolha da flag"}
    """
    client = load_gemini_client(env)
    response = client.models.generate_content(model="gemini-2.0-flash",contents=prompt+atendimento)
    result = {'result':response.text, 
              'raw':atendimento,
              'flag_rastreado': flag_rastreio}
    
    return result

@task
def parse_result_dataframe(list_result):
    def parse_result_row(dict_result):
        dict_result = dict_result.replace('\n','')
        regexp_flag = re.compile(r'.*{"flag": {0,1}(?P<flag>.*), "motivo": {0,1}(?P<motivo>.*)}.*')
        re_match = regexp_flag.match(dict_result)

        flag = re_match.group('flag')
        flag = flag.replace('"',"")
        flag = flag.replace('"',"")

        motivo = re_match.group('motivo')
        motivo = motivo.replace('"',"")
        motivo = motivo.replace('"',"")

        result = {"flag":flag, "motivo":motivo}

        return result
    
    df = pd.DataFrame(list_result)
    df['result_json'] = df['result'].apply(lambda x: parse_result_row(x))
    df = pd.json_normalize(df[['raw','flag_rastreado','result_json']].to_dict(orient='records'), max_level=1)
    df.rename({'result_json.flag':'flag_gemini','result_json.motivo':'motivo_gemini'},axis=1,inplace=True)
    
    return df