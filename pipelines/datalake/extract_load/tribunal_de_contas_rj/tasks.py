# -*- coding: utf-8 -*-
import datetime
import re
from datetime import timedelta
from typing import List, Optional

import pandas as pd
import pytz
from bs4 import BeautifulSoup

from pipelines.datalake.extract_load.tribunal_de_contas_rj.utils import (
    send_request,
    split_case_number,
    find_h5_from_text,
    get_table_rows_from_h5,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(seconds=30), nout=2)
def fetch_case_page(case_num: str, env: Optional[str] = None) -> List[str]:

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
        "POST", f"{HOST}{PATH}", {"Sec": sec, "Num": num, "Ano": year, "TipoConsulta": "PorNumero"}
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

    # TODO: pegar também o nome do processo. algo assim:
    # <div class="form-group">
    #     <label for="Processo_Objeto">Objeto</label>
    #     <p class="form-control-static">xxxx</p>
    # </div>
    return (case_num, html)


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def scrape_lastest_decision_from_page(case: str, page: BeautifulSoup) -> List[List[str]]:
    # Queremos pegar a tabela logo após "<h5>Decisões do Processo</h5>"
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
    h5_decisions = find_h5_from_text(page, r"decisões do processo")
    if h5_decisions is None:
        raise RuntimeError("Unable to find case rulings in page")

    rows = get_table_rows_from_h5(h5_decisions)
    if rows is None:
        raise RuntimeError("Unable to find case rulings in page")

    # Pega data e decisão a partir de linha de tabela
    def build_results_from_row(case: str, row: BeautifulSoup):
        values = [case]
        # Filhos são algo como: ["\n", <td>, "\n", <td>, "\n"]
        for child in row.children:
            # Pula text nodes (e.g. "\n")
            if child.name is None:
                continue
            values.append(child.get_text().strip())
        return values

    main_case = build_results_from_row(case, rows[0])

    # Alguns processos também têm 'processos apensos' (ex.: 009/001493/2020)
    h5_appendices = find_h5_from_text(page, r"processos apensos")
    # Se não encontramos o <h5>, então podemos ir embora
    if h5_appendices is None:
        # Ex.: values = ['000/100000/2010', '01/01/2000', 'Contas Regulares']
        return [main_case]  # Retorna array de arrays

    # Senão, então precisamos pegar também os apensos
    log("Found appendices; fetching...")

    # É similar, mas uma tabela que aparece depois de um de "<h5>Processos Apensos</h5>"
    # ...
    # <td> <a href="/processo/Ficha?ctid=1831051">009/004365/2016</a> </td>
    # <td> Prestação de Contas do Contrato de Gestão ... </td>
    # Precisamos pegar a decisão mais recente de cada um desses apensos
    rows = get_table_rows_from_h5(h5_appendices)
    if rows is None:
        log("Unable to find case appendices in page", level="warning")
        return [main_case]

    results = [main_case]
    # TODO: ai algo tipo
    # for row in rows:
    #     fetch_case_page.run(case_num="...")
    #     scrape_lastest_decision_from_page.run(...)?

    return results
