# -*- coding: utf-8 -*-
import re
from datetime import datetime
from typing import List, Optional

import pandas as pd
import pytz
from bs4 import BeautifulSoup

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake

from .utils import (
    cleanup_text,
    find_h5_from_text,
    get_counselors_initials,
    get_table_rows_from_h5,
    send_request,
    split_case_number,
)


@task(nout=2)
def fetch_case_page(case_num: str, env: Optional[str] = None) -> tuple:
    case_num = str(case_num).strip()
    log(f"Attempting to fetch '{case_num}'")

    (sec, num, year) = split_case_number(case_num)
    case_num = f"{sec}/{num}/{year}"

    if sec is None or num is None or year is None:
        raise RuntimeError(f"Error getting case number: {case_num}")

    # Precisamos mandar um POST enviando o seguinte FormData
    # : Sec=xxx&Num=xxxxxx&Ano=xxxx&TipoConsulta=PorNumero
    # Isso automaticamente redireciona pra um 'GET /processo/Ficha?Ctid=xxxxxx'
    HOST = "https://etcm.tcmrio.tc.br"
    PATH = "/processo/Lista"
    (url, html) = send_request(
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
        a = html.find("a", attrs={"href": re.compile(r"^/processo/Ficha\?Ctid=")})
        if a is None or not a:
            h5 = html.find("h5")
            if h5 is not None:
                log(h5.get_text().strip(), level="warning")
            raise RuntimeError("Failed to find main case ID")
        # Pega o caminho do URL
        path = a.get("href")
        # E fazemos um GET manual a ele
        (url, html) = send_request("GET", f"{HOST}{path}")

    ctid_regex = re.compile(r"ctid=(?P<ctid>[0-9]+)", re.IGNORECASE)
    m = ctid_regex.search(url)
    ctid = m.group("ctid")
    log(f"Detected 'ctid'='{ctid}'")
    # Salva data/hora em que a página foi extraída
    dt = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    case_obj = {
        "_extracted_at": dt,
        "_ctid": ctid,
        "processo_id": case_num,
    }
    return (case_obj, html)


@task()
def get_latest_vote(ctid: str):
    if not ctid:
        log("Received no ctid; skipping vote", level="warning")
        return None

    HOST = "https://etcm.tcmrio.tc.br"
    file_path = f"/InteiroTeor/Visualizar?ctid={ctid}"
    (_, html) = send_request("GET", f"{HOST}{file_path}")
    html: BeautifulSoup
    # Se existe <div> de erro
    div = html.find("div", {"class": "validation-summary-errors"})
    if div:
        # Se é o primeiro caso
        not_allowed_text = "não liberado para consulta"
        if not_allowed_text in div.get_text():
            log("Votes not available for this case", level="warning")
            return f"{HOST}/processo/Ficha?ctid={ctid}"
    # Caso contrário, presume que existe voto disponível, retorna o URL
    ## Infelizmente, o TCM foi mudado em ~fev/2026 e não mais permite que os
    ## arquivos sejam acessados diretamente via URL. Assim, não temos como
    ## obter o link para o voto do conselheiro, então nos limitamos a devolver
    ## o (novo) URL que lista arquivos ("peças") do processo.
    return f"{HOST}/InteiroTeor/Visualizar?ctid={ctid}"


@task()
def scrape_case_info_from_page(case_tuple: tuple) -> dict:
    assert len(case_tuple) == 2, f"Expected 2 parameters; got {len(case_tuple)}"
    # Extrai os parâmetros reais da tupla recebida
    case_obj: dict = case_tuple[0]
    page: BeautifulSoup = case_tuple[1]

    ###########################################################
    #  Cabeçalho
    ###########################################################

    # Queremos algumas informações do cabeçalho do processo
    # O formato é:
    # <div class="form-group">
    #   <label for="Processo_Objeto">Objeto</label>
    #   <p class="form-control-static">(texto que queremos)</p>
    # </div>
    def get_information_from_label(root: BeautifulSoup, label_for: str):
        # Por algum motivo, html.find("label", attrs={"for", "..."})
        # retorna None, mas .select(...) funciona
        label = root.select(f"label[for='{label_for}']")
        p = label[0].parent.find("p") if len(label) > 0 else None
        if p is None or not p:
            log(
                f"Failed to find case information (for='{label_for}'); will be 'null'",
                level="warning",
            )
            return None
        return cleanup_text(p.get_text())

    case_obj["objeto"] = get_information_from_label(page, "Processo_Objeto")
    case_obj["detalhes"] = get_information_from_label(page, "Processo_Detalhamento_ObjetoDetalhado")
    case_obj["valor"] = get_information_from_label(
        page, "Processo_IdentificacaoInstrumento_ValorComMoeda"
    )
    case_obj["autuacao_data"] = get_information_from_label(page, "Processo_Autuacao")
    case_obj["entrada_tcm_data"] = get_information_from_label(page, "Processo_Entrada")

    ###########################################################
    #  Tabelas
    ###########################################################

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
    def get_rows_from_text(root: BeautifulSoup, match: str, throw: Optional[str] = None):
        h5 = find_h5_from_text(root, match)
        if h5 is None:
            if throw is not None:
                raise RuntimeError(throw)
            return None
        rows = get_table_rows_from_h5(h5)
        if rows is None or len(rows) < 1:
            if throw is not None:
                raise RuntimeError(throw)
            return None
        return rows

    # Pega data e decisão a partir de linha de tabela
    def get_row_contents(row: BeautifulSoup) -> List[str]:
        values = []
        # Filhos são algo como: ["\n", <td>, "\n", <td>, "\n"]
        for child in row.children:
            # Pula text nodes (e.g. "\n")
            if child.name is None:
                continue
            values.append(cleanup_text(child.get_text()))
        return values

    rows = get_rows_from_text(
        page, r"decisões do processo", throw="Unable to find case rulings in page"
    )
    ruling = get_row_contents(rows[0])
    # Deve ser algo como ['01/01/2000', 'Contas Regulares']
    assert len(ruling) > 1, "Failed to get latest ruling from page!"
    case_obj["decisao_data"] = ruling[0]
    case_obj["decisao_texto"] = ruling[1]

    # Alguns processos possuem 'partes' interessadas; pegamos se existir
    rows = get_rows_from_text(page, r"^partes") or []
    parts = []
    prosecutors = []
    for row in rows:
        part = get_row_contents(row)
        if len(part) < 2:
            continue
        part_name = part[0].lower()
        if part_name == "parte":
            parts.append(part[1])
        elif part_name.startswith("advogado") or part_name.startswith("procurador"):
            prosecutors.append(part[1])
        else:
            log(f"Unknown part '{part}'; ignoring", level="warning")

    case_obj["partes"] = ";".join(parts) if len(parts) > 0 else None
    case_obj["procuradores"] = ";".join(prosecutors) if len(prosecutors) > 0 else None

    return case_obj


@task()
def upload_results(main_result: dict, latest_vote: str, dataset: str):
    # Remove propriedade de apoio
    del main_result["_ctid"]
    # Adiciona o URL do voto do conselheiro
    main_result["voto_conselheiro"] = latest_vote

    # Constrói DataFrame a partir do resultado principal
    main_df = pd.DataFrame.from_records([main_result])

    log(f"Uploading DataFrame: {len(main_df)} rows; columns {list(main_df.columns)}")
    # Chama a task de upload
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
