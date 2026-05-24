import re
from prefect import task
from datetime import timedelta
from hashlib import sha256
import requests
from requests import Session
from bs4 import BeautifulSoup, Tag
from datetime import datetime
import pandas as pd
import time
from typing import List, Dict, Tuple

from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg_preparos.constants import (
    SISREG_BASE_URL,
    REQUEST_HEADERS,
    SISREG_PREPAROS_URL,
    ETAPA_PREPARO,
    DEFAULT_UNIDADES_LIMIT
)

# Configurações globais


def fix_characters(datainfo: str | None) -> str:
    if not datainfo:
        return ""
    return re.sub(r"[;►–,]", " - ", datainfo, count=0)


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def login(user: str, senha: str) -> Session:
    senha = sha256(senha.upper().encode('utf-8')).hexdigest()
    payload = {'usuario': user, 'senha_256': senha, 'etapa': "ACESSO"}

    session = requests.Session()
    response = session.post(SISREG_BASE_URL, data=payload, headers=REQUEST_HEADERS)

    if "Efetue o logon novamente" in response.text:
        raise Exception("Erro no login. Verifique as credenciais.")

    return session


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_unidades(session: Session) -> List[Tag]:
    res = session.get(SISREG_PREPAROS_URL)
    soup = BeautifulSoup(res.text, 'lxml')
   # log(soup)

    form = soup.find('form')
   # log(form)

    unidades = (
        form.find('table').find_all('tr')[1].find_all('option')
        if form and form.find('table') and len(form.find('table').find_all('tr')) > 1
        else []
    )

   # log(unidades)
    return unidades


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_procedimentos(session: Session, unidade: Tag) -> Tuple[str, str, List[Tag]]:
    valor_unidade = unidade['value']
    nome_unidade = unidade.text.strip()

    params_proc = {'etapa': 'LISTAR_PROCEDIMENTOS', 'unidade': valor_unidade}
    res_proc = session.get(SISREG_PREPAROS_URL, params=params_proc)
    soup_proc = BeautifulSoup(res_proc.text, 'lxml')

    procedimentos = soup_proc.form.table('tr')[2]('option')
    return nome_unidade, valor_unidade, procedimentos


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def coletar_preparos(session: Session, nome_unidade: str,
                     valor_unidade: str,
                     procedimentos: List[Tag]) -> List[Dict[str, str]]:
    data = []

    for procedimento in procedimentos[1:]:
        valor_proc = procedimento.get('value', '')
        nome_proc = procedimento.text.strip()

        params_prep = {
            'etapa': ETAPA_PREPARO,
            'unidade': valor_unidade,
            'procedimento': valor_proc
        }

        res_prep = session.get(SISREG_PREPAROS_URL, params=params_prep)
        soup_prep = BeautifulSoup(res_prep.text, 'lxml')

        iframe = soup_prep.find('textarea', {'id': 'preparo'})

        descricao = iframe.get_text(strip=True) if iframe else ""

        data_details = {
            "cnes": valor_unidade,
            "nomeUnidade": nome_unidade,
            "codProcedimento": valor_proc,
            "nomeProcedimento": nome_proc,
            "descricaoPreparo": fix_characters(descricao),
          #  "dt_extracao": datetime.today()
        }

        data.append(data_details)

        log(data_details)
        # dentro da task
    # delay_request()
        time.sleep(8.5)
    return data


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def processar_unidades(session: Session, unidades: List[Tag], unidades_limit: int | None = DEFAULT_UNIDADES_LIMIT) -> pd.DataFrame:
    if unidades_limit is not None:
        unidades_iter = unidades[:unidades_limit]
    else:
        unidades_iter = unidades

    log(f"Processando {len(unidades_iter)} unidades (limite={unidades_limit})")

    all_data = []
    for unidade in unidades_iter:
        nome_unidade, valor_unidade, procedimentos = coletar_procedimentos.run(session, unidade)
        log(f"Unidade: {nome_unidade}, procedimentos encontrados: {len(procedimentos)}")
        for prep in coletar_preparos.run(session, nome_unidade, valor_unidade, procedimentos):
            log(f"Preparo coletado: {prep}")
            all_data.append(prep)

    expected_cols = ["cnes","nomeUnidade","codProcedimento","nomeProcedimento","descricaoPreparo"]
    df = pd.DataFrame(all_data, columns=expected_cols)
    df["dt_extracao"] = datetime.today().strftime("%Y-%m-%d")

    log(f"Total de preparos coletados: {len(df)}")
    return df
