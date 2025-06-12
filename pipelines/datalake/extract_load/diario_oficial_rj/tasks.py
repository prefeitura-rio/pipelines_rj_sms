# -*- coding: utf-8 -*-
import datetime
import re
import urllib.parse
from datetime import timedelta
from typing import List, Optional

import pandas as pd
import pytz
from bs4 import BeautifulSoup

from pipelines.datalake.extract_load.diario_oficial_rj.utils import (
    get_links_for_path,
    get_links_if_match,
    get_today,
    node_cleanup,
    parse_do_contents,
    send_get_request,
    standardize_date_from_string,
    string_cleanup,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_current_DO_identifiers(date: Optional[str], env: Optional[str]) -> List[str]:
    if date is not None:
        date = standardize_date_from_string(date)
    else:
        date = get_today()

    # Precisamos pegar o identificador do DO do dia de hoje
    # Para isso, fazemos uma busca na API no formato:
    # https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0/di:2025-06-02/df:2025-06-02/?q=%22rio%22

    BASE = "https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0/"
    DATE_INTERVAL = f"di:{date}/df:{date}/"
    # Precisamos de algum texto de busca, então buscamos
    # por "Secretaria Municipal de Saúde", entre aspas
    QUERY = "?q=" + urllib.parse.quote('"rio"')
    URL = f"{BASE}{DATE_INTERVAL}{QUERY}"
    log(f"Fetching DO for date '{date}'")

    # Faz requisição GET, recebe um JSON
    json = send_get_request(URL, "json")
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


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_article_names_ids(diario_id_date: tuple) -> List[tuple]:
    assert len(diario_id_date) == 2, "Tuple must be (id, date) pair!"
    diario_id = diario_id_date[0]
    diario_date = diario_id_date[1]

    URL = f"https://doweb.rio.rj.gov.br/portal/visualizacoes/view_html_diario/{diario_id}"
    log(f"Fetching articles for DO ID '{diario_id}'")
    # Faz requisição GET, recebe HTML
    html = send_get_request(URL, "html")

    # Precisamos encontrar todas as instâncias relevantes de
    # <a class="linkMateria" identificador="..." pagina="" data-id="..." data-protocolo="..." data-materia-id="...">
    # De onde queremos extrair `identificador` ou `data-materia-id`
    all_folders = html.find_all("span", attrs={"class": "folder"})
    results = []
    results.extend(get_links_for_path(all_folders, ["atos do prefeito", "decretos n"]))
    results.extend(
        get_links_for_path(
            all_folders, ["secretaria municipal de saúde", "resoluções", "resolução n"]
        )
    )
    results.extend(get_links_if_match(all_folders, r"^controladoria geral"))
    results.extend(get_links_if_match(all_folders, r"^tribunal de contas"))

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
    (do_id, do_date) = do_tuple[0]
    (folder_path, title, id) = do_tuple[1]

    log(f"Getting content of article '{title}' (id '{id}')...")
    URL = f"https://doweb.rio.rj.gov.br/apifront/portal/edicoes/publicacoes_ver_conteudo/{id}"
    # Faz requisição GET, recebe HTML
    html = send_get_request(URL, "html")
    # Talvez o resultado não seja HTML (pode ser PDF por exemplo)
    if html is None:
        return None
    # Registra data/hora da extração
    current_datetime = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    # Salva o HTML cru do corpo
    # body_html = string_cleanup(html.body)

    # Remove elementos inline comuns (<b>, <i>, <span>) pra não
    # atrapalhar o .get_text() com separador de \n abaixo
    # TODO: limpar essa função se não formos voltar com nada disso
    html = node_cleanup(html)
    content_list = parse_do_contents(html.body)

    # Usa .get_text() para pegar o conteúdo textual da página toda
    # full_text = string_cleanup(html.body.get_text(separator="\n", strip=True))

    log(f"Article '{title}' (id '{id}') has size {len(content_list)} block(s)")
    return [
        {
            "do_data": do_date,
            "do_id": do_id,
            "secao": folder_path,
            "titulo": title,
            "conteudo": content,
            "_extracted_at": current_datetime,
            # "texto": full_text,
            # "html": body_html,
        }
        for content in content_list
    ]


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def upload_results(results_list: List[dict], dataset: str):
    if len(results_list) == 0:
        log(f"Nothing to upload; leaving")
        return

    main_df = pd.DataFrame()

    # Para cada resultado
    for result in results_list:
        # Pula resultados 'vazios' (i.e. PDFs ao invés de texto, etc)
        if result is None:
            continue
        # Constrói DataFrame a partir do resultado
        single_df = pd.DataFrame.from_records([result])
        # Concatena com os outros resultados
        main_df = pd.concat([main_df, single_df], ignore_index=True)

    log(f"Uploading DataFrame: {len(main_df)} rows; columns {list(main_df.columns)}")
    # Chamando a task de upload
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
