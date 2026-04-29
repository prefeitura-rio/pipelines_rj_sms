from prefect import task
from datetime import timedelta
from hashlib import sha256
import requests
from requests import Session
from bs4 import BeautifulSoup, Tag
import pandas as pd
import time
from typing import List, Dict, Tuple, Any
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log

# Configurações globais
BASE_URL = "http://sisregiii.saude.gov.br/cgi-bin/config_preparo"
LOGIN_URL = "https://sisregiii.saude.gov.br"

headers = {
    "user-agent": "Mozilla/5.0",
    "content-type": "application/x-www-form-urlencoded"
}


def fix_characters(datainfo:str | None) -> str:
    if not datainfo:
        return ""
    return datainfo.replace(";", " - ").replace("►", " - ").replace("–", " - ").replace(",", " - ")


# ---------------- Tasks ---------------- #

@task(max_retries=3, retry_delay=timedelta(minutes=5))
def login(user, senha):
    senha = sha256(senha.upper().encode('utf-8')).hexdigest()

    payload = {
        'usuario': user,
        'senha_256': senha,
        'etapa': "ACESSO"
    }

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
    unidades = []

    if form and form.find('table'):
        rows = form.find('table').find_all('tr')
        if len(rows) > 1:
            unidades = rows[1].find_all('option')

    log(unidades)
    return unidades


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_procedimentos(session, unidade: Tag) -> Tuple[str, str, List[Tag]]:
    valor_unidade: str = unidade['value']
    nome_unidade: str = unidade.text.strip()

    params_proc: Dict[str, str] = {
        'etapa': 'LISTAR_PROCEDIMENTOS',
        'unidade': valor_unidade
    }

    res_proc = session.get(BASE_URL, params=params_proc)
    soup_proc = BeautifulSoup(res_proc.text, 'lxml')

    procedimentos = soup_proc.form.table('tr')[2]('option')

    return nome_unidade, valor_unidade, procedimentos


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_preparos(session: Session, nome_unidade: str, valor_unidade: str, procedimentos: List[Tag]) -> List[Dict[str, str]]:
    data: List[Dict[str, str]] = []

    for procedimento in procedimentos[1:]:
        nome_proc: str = procedimento.text.strip()
        valor_proc: str = procedimento['value']

        params_prep: Dict[str, str] = {
            'etapa': 'FRM_PREPARO',
            'unidade': valor_unidade,
            'procedimento': valor_proc
        }

        res_prep = session.get(BASE_URL, params=params_prep)
        soup_prep = BeautifulSoup(res_prep.text, 'lxml')

        iframe = soup_prep.find('textarea', {'id': 'preparo'})
        
         
        descricao = iframe.get_text(strip=True).strip() if iframe else ""

        data_details = {
            "cnes": valor_unidade,
            "nomeUnidade": nome_unidade,
            "codProcedimento": valor_proc,
            "nomeProcedimento": nome_proc,
            "descricaoPreparo": fix_characters(descricao)
        }

        
        data.append(data_details)
        log(data_details)

        time.sleep(8.5)

    return data


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def processar_unidades(session: Session, unidades: List[Tag]) -> List[Dict[str, str]]:
    all_data: List[Dict[str, str]] = []

    contadora_assassina = 0

    for unidade in unidades:
        if contadora_assassina > 1:
            break
        else:
            contadora_assassina +=1

    for unidade in unidades:
        nome_unidade, valor_unidade, procedimentos = coletar_procedimentos.run(session, unidade)
        data_unidade = coletar_preparos.run(session, nome_unidade, valor_unidade, procedimentos)
        all_data.extend(data_unidade)

    return all_data

# @task(max_retries=3, retry_delay=timedelta(minutes=5))
# def salvar_bigquery(data: List[Dict[str, Any]]) -> str:
#     client = bigquery.Client()

#     table_id: str = "seu_projeto.seu_dataset.preparos_unidades"

#     df = pd.DataFrame(data)

#     job = client.load_table_from_dataframe(df, table_id)
#     job.result()

#     return f"{len(df)} registros inseridos no BigQuery."