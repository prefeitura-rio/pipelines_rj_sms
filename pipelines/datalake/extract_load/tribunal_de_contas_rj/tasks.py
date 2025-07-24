# -*- coding: utf-8 -*-
import re
from datetime import datetime, timedelta
from time import sleep
from typing import Optional

import pandas as pd
import pytz
from bs4 import BeautifulSoup
from requests import HTTPError

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


@task(max_retries=3, retry_delay=timedelta(seconds=30), nout=2)
def fetch_case_page(case_num: str, env: Optional[str] = None) -> tuple:
    case_num = str(case_num).strip()
    log(f"Attempting to fetch '{case_num}'")

    (sec, num, year) = split_case_number(case_num)

    if sec is None or num is None or year is None:
        raise RuntimeError(f"Error getting case number: {sec}/{num}/{year}")

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
        a = html.find("a", attrs={"href": re.compile("^/processo/Ficha\?Ctid=")})
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


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_latest_vote(ctid: str):
    if not ctid:
        log("Received no ctid; skipping vote", level="warning")
        return None

    # Situação:
    # - Para pegar o voto, às vezes é preciso primeiro mandar um
    #   POST /InteiroTeor/Index?ctid=xxxx com FormData:
    #       Processo.ContainerID=xxxx (ctid)
    #       RecaptchaToken=xxxx, __RequestVerificationToken=xxxx
    # - Não temos como fazer isso por código porque requer Recaptcha
    # - Se você achar uma forma de burlar: GET /InteiroTeor/EstaLiberado?ctid=xxxx
    #   - Isso retorna ou:
    #     {"redirectUrl":"/InteiroTeor/Arquivos?ctid=xxxx"}
    #     - Já pode pegar o voto, GET /InteiroTeor/Arquivos?ctid=xxxx
    #   - Ou erro 404
    #     - Talvez o voto exista mas precise do POST; talvez não exista

    # - Como estamos bloqueados pelo Recaptcha, pegamos direto /Arquivos
    # - Pode retornar um <div> com erros:
    #   - "Processo não liberado para consulta."
    #       - Não temos como pegar votos desse processo; retorna nulo
    #   - "O inteiro teor desse processo foi solicitado mas ainda não está disponível.
    #      Por favor, aguarde mais alguns minutos e refaça a consulta."
    #       - Pegar os votos requer o POST; retorna o URL do botão
    # - Se não houver <div> de erro, então melhor caso: podemos pegar o URL do PDF
    HOST = "https://etcm.tcmrio.tc.br"
    file_path = f"/InteiroTeor/Arquivos?ctid={ctid}"
    (_, html) = send_request("GET", f"{HOST}{file_path}")
    html: BeautifulSoup
    # Se existe <div> de erro
    div = html.find("div", {"class": "validation-summary-errors"})
    if div:
        # Se é o primeiro caso
        not_allowed_text = "não liberado para consulta"
        if not_allowed_text in div.get_text():
            log("Votes not available for this case", level="warning")
            return None
        # Caso contrário, presume que existe voto disponível, retorna o URL
        log("Votes are available but unreachable for this case", level="warning")
        return f"{HOST}/InteiroTeor/Index?ctid={ctid}"

    # Se estamos aqui, então provavelmente temos como pegar o URL do PDF
    # Portanto, precisamos das iniciais dos conselheiros
    initials = get_counselors_initials()

    def get_table_contents_from_h5_text(
        root: BeautifulSoup, match: str, throw: Optional[str] = None
    ):
        # Encontra <h5> com o conteúdo textual especificado
        h5 = find_h5_from_text(root, match)
        if h5 is None:
            if throw is not None:
                raise RuntimeError(throw)
            return None
        # Pega <table> logo após o <h5> (nessa página são irmãos imediatos)
        table = h5.find_next_sibling("table")
        # Pega todos os <tr> dentro da tabela
        rows = table.find_all("tr")
        if rows is None or len(rows) < 1:
            if throw is not None:
                raise RuntimeError(throw)
            return None

        table_contents = []
        # Para cada <tr>, de baixo para cima
        for row in reversed(rows):
            values = []
            # Filhos são algo como: ["\n", <td>, "\n", <td>, "\n"]
            for child in row.children:
                # Pula text nodes (e.g. "\n")
                if child.name is None:
                    continue
                # Se existe um <a href="...download..."> no elemento
                a = child.find("a", attrs={"href": re.compile("download", re.IGNORECASE)})
                if a:
                    # Pega `href` dele
                    values.append(f"{HOST}{a.get('href')}")
                    continue
                # Caso contrário, pega o conteúdo textual do elemento
                values.append(child.get_text().strip())
            table_contents.append(values)
        return table_contents

    contents = get_table_contents_from_h5_text(
        html, r"^peças do processo", throw="Unable to find case pieces in page"
    )

    for row in contents:
        # [ Ordem, Descrição, Extensão, Tamanho, Download ]
        # Estamos em busca de algo como 'GCS-5 DCPN / 184 / 2024',
        # onde DCPN é uma sigla de conselheiro
        # Queremos, portanto:
        # - Para cada descrição, pegar as 3 primeiras palavras (separadas por espaço)
        description = row[1]
        download = row[4]

        first_words = description.split(" ")[:3]
        for word in first_words:
            # Se encontramos uma delas na lista de siglas
            if word in initials:
                log(f"Found initials '{word}' in '{description}'")
                # Pega o URL de download
                return download

    log("Could not find counselor vote", level="warning")
    return None


@task(max_retries=3, retry_delay=timedelta(seconds=30))
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
    def get_row_contents(row: BeautifulSoup):
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
        if part[0].lower() == "parte":
            parts.append(part[1])
        elif part[0].lower().startswith("advogado"):
            prosecutors.append(part[1])
        else:
            log(f"Unknown part {part}; ignoring", level="warning")

    case_obj["partes"] = ";".join(parts) if len(parts) > 0 else None
    case_obj["procuradores"] = ";".join(prosecutors) if len(prosecutors) > 0 else None

    return case_obj


@task(max_retries=3, retry_delay=timedelta(seconds=30))
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
