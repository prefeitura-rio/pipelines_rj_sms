from prefect import task
from datetime import timedelta
from hashlib import sha256
import requests
from requests import Session
from bs4 import BeautifulSoup, Tag
from datetime import datetime
import pandas as pd
import time
from typing import List, Dict, Tuple, Any
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.flow import Flow 
#from constants import delay_request


# Configurações globais

BASE_URL = "http://sisregiii.saude.gov.br/cgi-bin/config_preparo"
LOGIN_URL = "https://sisregiii.saude.gov.br"

headers = {
    "user-agent": "Mozilla/5.0",
    "content-type": "application/x-www-form-urlencoded"
}


#def fix_characters(datainfo: str | None) -> str:
#    return "" if not datainfo else datainfo.replace(";", " - ").replace("►", " - ").replace("–", " - ").replace(",", " - ")

import re

def fix_characters(datainfo: str | None) -> str:
    if not datainfo:
        return ""
    return re.sub(r"[;►–,]", " - ", datainfo, count=0)


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def login(user: str, senha: str) -> Session:
    senha = sha256(senha.upper().encode('utf-8')).hexdigest()
    payload: Dict[str, str] = {'usuario': user, 'senha_256': senha, 'etapa': "ACESSO"}

    session = requests.Session()
    response = session.post(LOGIN_URL, data=payload, headers=headers)

    if "Efetue o logon novamente" in response.text:
        raise Exception("Erro no login. Verifique as credenciais.")

    return session


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_unidades(session: Session) -> List[Tag]:
    res = session.get(BASE_URL)
    soup = BeautifulSoup(res.text, 'lxml')
    log(soup)

    form = soup.find('form')
    log(form)

    unidades: List[Tag] = (
        form.find('table').find_all('tr')[1].find_all('option')
        if form and form.find('table') and len(form.find('table').find_all('tr')) > 1
        else []
    )

   # log(unidades)
    return unidades


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_procedimentos(session: Session, unidade: Tag) -> Tuple[str, str, List[Tag]]:
    valor_unidade: str = unidade['value']
    nome_unidade: str = unidade.text.strip()

    params_proc: Dict[str, str] = {'etapa': 'LISTAR_PROCEDIMENTOS', 'unidade': valor_unidade}
    res_proc = session.get(BASE_URL, params=params_proc)
    soup_proc = BeautifulSoup(res_proc.text, 'lxml')

    procedimentos: List[Tag] = soup_proc.form.table('tr')[2]('option')
    return nome_unidade, valor_unidade, procedimentos


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_preparos(session: Session, nome_unidade: str, valor_unidade: str, procedimentos: List[Tag]) -> List[Dict[str, str]]:
    data: List[Dict[str, str]] = []

    for procedimento in procedimentos[1:]:
        valor_proc = procedimento.get('value', '')
        nome_proc = procedimento.text.strip()

        params_prep = {
            'etapa': 'FRM_PREPARO',
            'unidade': valor_unidade,
            'procedimento': valor_proc
        }

        res_prep = session.get(BASE_URL, params=params_prep)
        soup_prep = BeautifulSoup(res_prep.text, 'lxml')

        iframe = soup_prep.find('textarea', {'id': 'preparo'})

        descricao = iframe.get_text(strip=True) if iframe else ""

        

       #df_preparos_ajustados = handle_columns_to_bq(df=df_preparos)

        data_details = {
            "cnes": valor_unidade,
            "nomeUnidade": nome_unidade,
            "codProcedimento": valor_proc,
            "nomeProcedimento": nome_proc,
            "descricaoPreparo": fix_characters(descricao),
            "dt_extracao": datetime.today()
        }

       # data.append(data_details)
    
    
    
    
        log(data_details)
        # dentro da task
    #delay_request()
    time.sleep(8.5)
    return data




# @task(max_retries=3, retry_delay=timedelta(minutes=5))
# def coletar_preparos(session: Session, nome_unidade: str, valor_unidade: str, procedimentos: List[Tag]) -> List[Dict[str, str]]:
    
#     res_prep = session.get(BASE_URL, params=params_prep)
#     soup_prep = BeautifulSoup(res_prep.text, 'lxml')

#     iframe = soup_prep.find('textarea', {'id': 'preparo'})
        
         
#     descricao = iframe.get_text(strip=True).strip() if iframe else ""
    
#     data: List[Dict[str, str]] = [
       
#         {
           
#             "cnes": valor_unidade,
#             "nomeUnidade": nome_unidade,
#             "codProcedimento": procedimento['value'],
#             "nomeProcedimento": procedimento.text.strip(),
#             "descricaoPreparo": fix_characters(descricao)
#            #     BeautifulSoup(
#            #         session.get("http://sisregiii.saude.gov.br" + iframe['src']).text, 'lxml'
#            #     ).body.get_text(separator=" ").strip()
#            #     if (iframe := BeautifulSoup(
#            #         session.get(BASE_URL, params={
#            #             'etapa': 'FRM_PREPARO',
#            #             'unidade': valor_unidade,
#            #             'procedimento': procedimento['value']
#            #         }).text, 'lxml'
#            #     ).find('iframe', {'name': 'preparo_ifr'})) else ""
#            # )
#         }
#         for procedimento in procedimentos[1:]
#     ]

#     [log(item) or time.sleep(8.5) for item in data]  # mantém logging e delay
#     return data


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def processar_unidades(session: Session, unidades: List[Tag]) -> pd.DataFrame:
    # limita a 10 unidades
    unidades_limitadas = [u for i, u in enumerate(unidades) if i <= 2]

    all_data: List[Dict[str, str]] = [
        prep
        for unidade in unidades_limitadas
        for nome_unidade, valor_unidade, procedimentos in [coletar_procedimentos.run(session, unidade)]
        for prep in coletar_preparos.run(session, nome_unidade, valor_unidade, procedimentos)
       # all_data.extend(data_unidade)
        ]

    # Converte lista de dicionários em DataFrame
    df = pd.DataFrame(all_data)

    # Adiciona coluna de extração com a data atual
    df["dt_extracao"] = datetime.today().strftime("%Y-%m-%d")

    return df



# def salvar_bigquery(data: List[Dict[str, Any]]) -> str:
#     client = bigquery.Client()
#     table_id: str = "seu_projeto.seu_dataset.preparos_unidades"

#     df = pd.DataFrame(data)
#     job = client.load_table_from_dataframe(df, table_id)
#     job.result()

#     return f"{len(df)} registros inseridos no BigQuery."
