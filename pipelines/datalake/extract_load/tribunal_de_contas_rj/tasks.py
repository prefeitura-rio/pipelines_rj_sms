# -*- coding: utf-8 -*-
import datetime
import re

from datetime import timedelta
from typing import List, Optional
from bs4 import BeautifulSoup

import pandas as pd
import pytz

from pipelines.datalake.extract_load.tribunal_de_contas_rj.utils import (
    send_request,
    split_case_number,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def fetch_case_page(case_num: str, env: Optional[str]) -> List[str]:

    log(f"Fetching '{case_num}'")

    (sec, num, year) = split_case_number(case_num)

    if sec is None or num is None or year is None:
        raise RuntimeError(f"Error getting case number: {sec}/{num}/{year}")

    # Precisamos mandar um POST enviando o FormData
    # : Sec=xxx&Num=xxxxxx&Ano=xxxx&TipoConsulta=PorNumero
    # Isso automaticamente redireciona pra um 'GET /processo/Ficha?Ctid=xxxxxx'
    HOST = "https://etcm.tcmrio.tc.br"
    PATH = "/processo/Lista"
    html = send_request(
        "POST",
        f"{HOST}{PATH}",
        {"Sec": sec, "Num": num, "Ano": year, "TipoConsulta": "PorNumero"}
    )

    # Alguns processos estão vinculados a várias "referências", então abrem um
    # resultado de busca ao invés da página do processo diretamente
    # Ex.: 009/002324/2019, 040/100420/2019 (126!!)
    # Como detectar isso?
    # : <title>TCMRio - Portal do TCMRio  - Resultado de Pesquisa a Processos</title>
    # Outra opção: <h5>Foram encontrados {xxx} processos.</h5>
    title = html.find("title").get_text().strip().lower()
    if title.endswith("resultado de pesquisa a processos"):
        log("Case has references; attempting to GET main case contents")
        # Meu entendimento é que só importa o processo em si, e não as referências
        # A página retorna uma tabela com o processo na primeira linha e as
        # referências nas linhas seguintes
        # Pegamos o primeiro 'a[href^="/processo/Ficha?Ctid="]' da página
        a = html.find("a", attrs={"href": re.compile("^/processo/Ficha\?Ctid=")})
        if a is None or not a:
            raise RuntimeError("Failed to find main case ID")
        # Pega o caminho do URL
        path = a.get("href")
        # E fazemos um GET manual a ele
        html = send_request("GET", f"{HOST}{path}")

    return html


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def scrape_decision_from_page(page: BeautifulSoup, env: Optional[str]) -> List[str]:
    pass
    # A gente provavelmente quer pegar a tabela logo após "<h5>Decisões do Processo</h5>"
    # O formato é:
    # <div class="row">
    #   <div class="col-md-12">
    #     <h5>Decisões do Processo</h5>
    #   </div>
    # </div>
    # <div class="row">
    #   <div class="col-md-12 form-group">
    #     <table class="table table-striped ...">
    #       <thead> ... </thead>
    #       <tbody>
    #         <tr>
    #           <td>dd/mm/yyyy</td>
    #           <td>(texto que queremos)</td>

    # Alguns processo também têm 'processos apensos'
    # Ex: 009/001493/2020
    # É similar, mas uma tabela que aparece depois de um de "<h5>Processos Apensos</h5>"
    # ...
    # <tr>
    #   <td>
    #     <a href="/processo/Ficha?ctid=1831051">009/004365/2016</a>
    # E acredito que teria que pegar a última decisão de cada um desses apensos
