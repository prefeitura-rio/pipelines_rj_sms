# -*- coding: utf-8 -*-
import re
from datetime import datetime, timedelta
from typing import List, Optional, Union

import pandas as pd
import pytz
from bs4 import BeautifulSoup

from pipelines.datalake.extract_load.tribunal_de_contas_rj.utils import (
    find_h5_from_text,
    get_table_rows_from_h5,
    send_request,
    split_case_number,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def fetch_case_page(case_num: str, env: Optional[str] = None) -> tuple:

    log(f"Fetching '{case_num}'")

    (sec, num, year) = split_case_number(case_num)

    if sec is None or num is None or year is None:
        raise RuntimeError(f"Error getting case number: {sec}/{num}/{year}")

    # Precisamos mandar um POST enviando o seguinte FormData
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

    # Salva data/hora em que a página foi extraída
    dt = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    # Por fim, também queremos o nome do processo. O formato é:
    # <div class="form-group">
    #   <label for="Processo_Objeto">Objeto</label>
    #   <p class="form-control-static">(texto que queremos)</p>
    # </div>
    name = None
    # Por algum motivo, html.find("label", attrs={"for", "Processo_Objeto"})
    # retorna None, mas .select(...) funciona
    label = html.select("label[for='Processo_Objeto']")
    p = label[0].parent.find("p") if len(label) > 0 else None
    if p is None or not p:
        log("Failed to find case name! Will be 'null'", level="error")
    else:
        name = p.get_text().strip()

    # Ex.: (
    #   '009/004365/2016',
    #   'Prestação de Contas do Contrato ...',
    #   <datetime>,
    #   <BeautifulSoup>
    # )
    return (case_num, name, dt, html)


@task(max_retries=3, retry_delay=timedelta(seconds=30), nout=2)
def scrape_lastest_decision_from_page(case_tuple: tuple) -> List[Union[dict, List[str]]]:
    assert len(case_tuple) == 4, f"Expected 4 parameters; got {len(case_tuple)}"
    # Extrai os parâmetros reais da tupla recebida
    case_num: str = case_tuple[0]
    name: str = case_tuple[1]
    extracted_at: datetime = case_tuple[2]
    page: BeautifulSoup = case_tuple[3]

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
    if rows is None or len(rows) < 1:
        raise RuntimeError("Unable to find case rulings in page")

    # Pega data e decisão a partir de linha de tabela
    def get_row_contents(row: BeautifulSoup):
        values = []
        # Filhos são algo como: ["\n", <td>, "\n", <td>, "\n"]
        for child in row.children:
            # Pula text nodes (e.g. "\n")
            if child.name is None:
                continue
            values.append(child.get_text().strip())
        return values

    decision = get_row_contents(rows[0])
    # Deve ser algo no formato ['01/01/2000', 'Contas Regulares']
    assert len(decision) > 1, "Failed to get latest decision from page!"
    result = {
        "titulo": name,
        "processo_id": case_num,
        "decisao_data": decision[0],
        "decisao_texto": decision[1],
        "apensos": None,
        "_extracted_at": extracted_at,
    }

    # Alguns processos também têm 'processos apensos' (ex.: 009/001493/2020)
    h5_appendices = find_h5_from_text(page, r"processos apensos")
    # Se não encontramos o <h5>, então podemos ir embora
    if h5_appendices is None:
        return [result, []]

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
        return [result, []]

    appendices = []
    for row in rows:
        appendix = get_row_contents(row)
        if len(appendix) <= 0:
            log("Failed to get appendix case number!", level="error")
            continue
        # Aqui a princípio já temos o ID 'interno' do caso (ctid),
        # mas teria que reimplementar a task toda de fetch pra receber
        # também além do formato xxx/xxxxx/xxxx; então, dane-se
        appendices.append(appendix[0])

    # Salva referências aos apensos no resultado principal
    result["apensos"] = ";".join(appendices)
    return [result, appendices]


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def upload_results(main_result: dict, appendix_results: List[dict], dataset: str):
    # Constrói DataFrame a partir do resultado principal
    main_df = pd.DataFrame.from_records([main_result])

    falttened_results = []
    for sublist in appendix_results:
        falttened_results.extend(sublist)
    # Para cada resultado
    for result in falttened_results:
        # Pula resultados 'vazios' caso existam
        if result is None:
            continue
        # Constrói DataFrame a partir do apenso
        single_df = pd.DataFrame.from_records([result])
        # Concatena com os outros resultados
        main_df = pd.concat([main_df, single_df], ignore_index=True)

    # FIXME
    pd.set_option("display.max_columns", None)
    log(f"Uploading DataFrame: {len(main_df)} rows; columns {list(main_df.columns)}")
    # Chamando a task de upload
    upload_df_to_datalake.run(
        df=main_df,
        dataset_id=dataset,
        table_id="processos_tcm",
        partition_column="_extracted_at",
        if_exists="append",
        if_storage_data_exists="append",
        source_format="csv",
        csv_delimiter=",",
    )
