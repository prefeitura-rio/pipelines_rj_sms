# -*- coding: utf-8 -*-
import datetime
import urllib.parse
from datetime import timedelta
from typing import List, Optional

import pandas as pd
import pytz
from bs4 import BeautifulSoup

from pipelines.datalake.extract_load.diario_oficial_rj.utils import (
    get_links_for_path,
    node_cleanup,
    parse_do_contents,
    report_extraction_status,
    send_get_request,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake
from pipelines.utils.time import parse_date_or_today


@task(max_retries=3, retry_delay=timedelta(minutes=10))
def get_current_DO_identifiers(date: Optional[str], env: Optional[str]) -> List[str]:
    date = parse_date_or_today(date).strftime("%Y-%m-%d")

    # Precisamos pegar o identificador do DO do dia de hoje
    # Para isso, fazemos uma busca na API no formato:
    # https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0/di:2025-06-02/df:2025-06-02/?q=%22rio%22

    BASE = "https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0"
    DATE_INTERVAL = f"/di:{date}/df:{date}"
    # Precisamos de algum texto de busca, então buscamos
    # por "Secretaria Municipal de Saúde", entre aspas
    QUERY = "/?q=" + urllib.parse.quote('"rio"')
    URL = f"{BASE}{DATE_INTERVAL}{QUERY}"
    log(f"Fetching DO for date '{date}'")

    # Faz requisição GET, recebe um JSON
    json = send_get_request(URL, "json")
    # Confere se houve erro na requisição
    if isinstance(json, Exception):
        # Se sim, reporta status de falha na extração para essa data
        report_extraction_status(False, date, environment=env)
        # Dá erro; task vai ser retentada daqui a N minutos
        raise json

    # Resposta é um JSON com todas as instâncias encontradas da busca
    # Porém temos campos de metadadaos que indicam quantas instâncias foram encontradas
    # em cada edição, e isso nos dá os IDs que queremos:
    distinct_ids = set()
    for edition in json["aggregations"]["Edicoes"]["buckets"]:
        distinct_ids.add(edition["key"])
    distinct_ids = list(distinct_ids)
    do_count = len(distinct_ids)
    log(f"Found {do_count} distinct DO(s) on specified date: {distinct_ids}")
    # Atrela os IDs à data atual
    result = list(zip(distinct_ids, [date] * do_count))
    return result


@task(max_retries=3, retry_delay=timedelta(minutes=10))
def get_article_names_ids(diario_id_date: tuple) -> List[tuple]:
    assert len(diario_id_date) == 2, "Tuple must be (id, date) pair!"
    diario_id = diario_id_date[0]

    URL = f"https://doweb.rio.rj.gov.br/portal/visualizacoes/view_html_diario/{diario_id}"
    log(f"Fetching articles for DO ID '{diario_id}'")
    # Faz requisição GET, recebe HTML
    html = send_get_request(URL, "html")
    # Confere se houve erro na requisição
    if isinstance(html, Exception):
        # Dá erro; task vai ser retentada daqui a N minutos
        raise html

    # Precisamos encontrar todas as instâncias relevantes de
    # <a class="linkMateria" identificador="..." pagina="" data-id="..." data-protocolo="..." data-materia-id="..."> # noqa
    # De onde queremos extrair `identificador` ou `data-materia-id`
    all_folders = html.find_all("span", attrs={"class": "folder"})
    results = []
    paths = [
        ["atos do prefeito", "decretos n"],
        ["secretaria municipal de saúde", "resoluções", "resolução n"],
        ["controladoria geral do município do rio de janeiro", "resoluções", "resolução n"],
        ["tribunal de contas do município", "resoluções", "resolução n"],
        ["tribunal de contas do município", "outros"],
        ["avisos editais e termos de contratos", "secretaria municipal de saúde", "avisos"],
        ["avisos editais e termos de contratos", "secretaria municipal de saúde", "outros"],
        [
            "avisos editais e termos de contratos",
            "controladoria geral do município do rio de janeiro",
            "outros",
        ],
        ["avisos editais e termos de contratos", "tribunal de contas do município", "outros"],
    ]
    for path in paths:
        results.extend(get_links_for_path(all_folders, path))

    # Não temos como garantir que ambos os atributos vão existir sempre;
    # então pegamos o valor do primeiro preenhcido que encontrarmos
    def get_any_attribute(tag, attr_list):
        for attr in attr_list:
            val = tag.attrs.get(attr)
            if val is None:
                continue
            val = val.strip()
            if len(val):
                return val
        return None

    def get_folder_path(tag: BeautifulSoup):
        path = []
        while True:
            tag = tag.parent
            if tag is None or not tag or tag.attrs.get("id") == "tree":
                break
            folder = tag.findChild("span", attrs={"class", "folder"}, recursive=False)
            if folder is None or not folder:
                continue
            path.append(folder.text.strip())
        return "/".join(reversed(path))

    # Cria lista de par (título, ID); título é guardado direto no banco
    # e ID é usado pra pegar o conteúdo textual/HTML do artigo
    filtered_results = list(
        set(
            (
                get_folder_path(tag),
                tag.text.strip(),
                get_any_attribute(tag, ["identificador", "data-materia-id"]),
            )
            for tag in results
        )
    )
    log(f"Found {len(filtered_results)} relevant articles")
    return [(diario_id_date, result) for result in filtered_results]


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_article_contents(do_tuple: tuple) -> List[dict]:
    assert len(do_tuple) == 2, "Tuple must be ((do_id, date), (title, id)) pair!"

    # Caso contrário, pega dados da etapa anterior
    (do_id, do_date) = do_tuple[0]
    (folder_path, title, id) = do_tuple[1]

    log(f"Getting content of article '{title}' (id '{id}')...")
    URL = f"https://doweb.rio.rj.gov.br/apifront/portal/edicoes/publicacoes_ver_conteudo/{id}"
    # Faz requisição GET, recebe HTML
    html = send_get_request(URL, "html")
    # Talvez o resultado não seja HTML (pode ser PDF por exemplo)
    if html is None:
        return None

    # Confere se houve erro na requisição
    if isinstance(html, Exception):
        # Se sim, retorna um valor que possamos detectar posteriormente
        return {"error": True}

    # Registra data/hora da extração
    current_datetime = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    base_result = {
        "_extracted_at": current_datetime,
        "do_data": do_date,
        "do_id": do_id,
        "materia_id": id,
        "secao": folder_path,
        "titulo": title,
    }

    # Remove elementos inline comuns (<b>, <i>, <span>)
    clean_html = node_cleanup(html)
    # Faz parsing do conteúdo para deixar tudo estruturado
    content_list = parse_do_contents(clean_html.body)

    log(f"Article '{title}' (id '{id}') has {len(content_list)} block(s)")
    ret = {
        **base_result,
        "sections": [
            {
                "secao_indice": section_index,
                "bloco_indice": block_index,
                "conteudo_indice": content_index,
                "cabecalho": content["header"],
                "conteudo": body,
            }
            for section_index, content in enumerate(content_list)
            for block_index, block in enumerate(content["body"])
            for content_index, body in enumerate(block)
        ],
    }
    return ret


@task(max_retries=1, retry_delay=timedelta(seconds=30))
def upload_results(results_list: List[dict], dataset: str, date: Optional[str], env: Optional[str]):
    if len(results_list) == 0:
        log("Nothing to upload; leaving")
        return

    main_df = pd.DataFrame()

    # Para cada resultado
    for result in results_list:
        # Pula resultados 'vazios' (i.e. PDFs ao invés de texto, etc)
        if result is None:
            continue
        # Confere se tivemos erro em alguma requisição
        if "error" in result:
            # Se sim, reporta status de falha na extração para essa data
            report_extraction_status(False, date, environment=env)
            # E aborta o upload dos resultados
            return

        # Para cada seção no resultado
        for section in result["sections"]:
            # Constrói objeto a ser upado com informações base
            # Remove `sections` (obviamente)
            prepped_section = {**{k: v for k, v in result.items() if k != "sections"}, **section}
            # Constrói DataFrame a partir do objeto
            single_df = pd.DataFrame.from_records([prepped_section])
            # Concatena com os outros resultados
            main_df = pd.concat([main_df, single_df], ignore_index=True)

    log(f"Uploading main DataFrame: {len(main_df)} rows; columns {list(main_df.columns)}")
    # Chama a task de upload
    upload_df_to_datalake.run(
        df=main_df,
        dataset_id=dataset,
        table_id="diarios_municipio",
        partition_column="_extracted_at",
        if_exists="append",
        if_storage_data_exists="append",
        source_format="csv",
        csv_delimiter=",",
    )

    # Se chegamos aqui, reporta status de sucesso na extração para essa data
    report_extraction_status(True, date, environment=env)
