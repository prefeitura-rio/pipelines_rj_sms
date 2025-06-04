# -*- coding: utf-8 -*-
import datetime
import re
import urllib.parse
from datetime import timedelta
from typing import List, Optional

import pandas as pd
import pytz

from pipelines.datalake.extract_load.diario_oficial_rj.utils import (
    get_links_for_path,
    get_links_if_match,
    get_today,
    node_cleanup,
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

    log(f"Found {len(distinct_ids)} distinct DO(s) on specified date: {distinct_ids}")
    return distinct_ids


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_article_names_ids(diario_id: str) -> List[tuple]:
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

    # Cria lista de par (título, ID); título é guardado direto no banco
    # e ID é usado pra pegar o conteúdo textual/HTML do artigo
    filtered_results = list(
        set(
            (tag.text.strip(), get_any_attribute(tag, ["identificador", "data-materia-id"]))
            for tag in results
        )
    )
    log(f"Found {len(filtered_results)} relevant articles")
    return filtered_results


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_article_contents(article: tuple) -> dict:
    assert len(article) == 2, "Tuple must be (title, id) pair!"

    title = article[0]
    id = article[1]

    log(f"Getting content of article '{title}' (id '{id}')...")
    URL = f"https://doweb.rio.rj.gov.br/apifront/portal/edicoes/publicacoes_ver_conteudo/{id}"
    # Faz requisição GET, recebe HTML
    html = send_get_request(URL, "html")
    # Registra data/hora da extração
    current_datetime = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    # Salva o HTML cru do corpo
    body_html = string_cleanup(html.body)
    # Remove elementos inline comuns (<b>, <i>, <span>) pra não
    # atrapalhar o .get_text() com separador de \n abaixo
    html = node_cleanup(html)
    # Usa .get_text() para pegar o conteúdo textual da página toda
    full_text = string_cleanup(html.body.get_text(separator="\n", strip=True))

    log(f"Article '{title}' (id '{id}') has size {len(full_text)} ({len(body_html)} with HTML)")
    return {
        "titulo": title,
        "texto": full_text,
        "html": body_html,
        "_extracted_at": current_datetime,
    }


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def upload_results(results_list: List[dict], dataset: str):
    if len(results_list) == 0:
        log(f"Nothing to upload; leaving")
        return

    main_df = pd.DataFrame()

    for result in results_list:
        single_df = pd.DataFrame.from_records([result])
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
    )
