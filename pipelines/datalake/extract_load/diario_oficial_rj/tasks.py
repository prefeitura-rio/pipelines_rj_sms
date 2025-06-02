# -*- coding: utf-8 -*-
from datetime import timedelta
import datetime
from typing import List, Optional
import urllib.parse
import pandas as pd
import re

import pytz

from pipelines.datalake.extract_load.diario_oficial_rj.utils import (
    send_get_request,
    standardize_date_from_string,
    get_today,
    string_cleanup,
    node_cleanup
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
    # https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0/di:2025-06-02/df:2025-06-02/?q=%22Secretaria%20Municipal%20de%20Sa%C3%BAde%22

    BASE = "https://doweb.rio.rj.gov.br/busca/busca/buscar/query/0/"
    DATE_INTERVAL = f"di:{date}/df:{date}/"
    # Precisamos de algum texto de busca, então buscamos
    # por "Secretaria Municipal de Saúde", entre aspas
    QUERY = "?q=" + urllib.parse.quote(
        '"Secretaria Municipal de Saúde"'
    )
    URL = f"{BASE}{DATE_INTERVAL}{QUERY}"
    log(f"Fetching DO for date '{date}'")

    # Faz requisição GET, recebe um JSON
    json = send_get_request(URL, "json")
    # Resposta é um JSON com todas as instâncias encontradas da busca
    # Porém temos campos de metadadaos que indicam quantas instâncias foram encontradas
    # em cada edição, e isso nos dá os IDs que queremos:
    distinct_ids = set()
    for id in json["aggregations"]["Edicoes"]["buckets"]:
        distinct_ids.add(id["key"])
    distinct_ids = list(distinct_ids)

    log(f"Found {len(distinct_ids)} distinct DO(s) on specified date: {distinct_ids}")
    return distinct_ids


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_article_names_ids(diario_id: str, test: bool) -> List[tuple]:
    URL = f"https://doweb.rio.rj.gov.br/portal/visualizacoes/view_html_diario/{diario_id}"
    log(f"Fetching articles for DO ID '{diario_id}'")
    # Faz requisição GET, recebe HTML
    html = send_get_request(URL, "html")

    # Precisamos encontrar todas as instâncias de
    # <a class="linkMateria" identificador="..." pagina="" data-id="..." data-protocolo="..." data-materia-id="...">
    # De onde vamos extrair `identificador` ou `data-materia-id`
    REGEX_DIGITS = re.compile(r"[0-9]+")
    results = set(
        html.find_all("a", attrs={ "identificador": REGEX_DIGITS })
        + html.find_all("a", attrs={ "data-materia-id": REGEX_DIGITS })
    )

    def get_any_attribute(tag, attr_list):
        for attr in attr_list:
            val = tag.attrs.get(attr)
            if val is None:
                continue
            val = val.strip()
            if len(val):
                return val
        return None

    # TODO: ver com a Natacha se queremos realmente só os decretos (:D), ou se queremos tudo (:c)
    filtered_results = set(
        (
            tag.text.strip(),
            get_any_attribute(tag, ["identificador", "data-materia-id"])
        )
        for tag in results
        if re.search(r"decreto rio", tag.text, re.IGNORECASE)
    )
    log(f"Found {len(results)} articles; filtered down to {len(filtered_results)} decrees")

    return list(filtered_results)


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_article_contents(obj: tuple, test: bool):
    # FIXME: por algum motivo o map() não ta funcionando, e essa função
    # recebe uma lista de tuplas ao invés de uma tupla
    if len(obj) > 0:
        obj = obj[0]

    title = obj[0]
    id = obj[1]

    URL = f"https://doweb.rio.rj.gov.br/apifront/portal/edicoes/publicacoes_ver_conteudo/{id}"
    # Faz requisição GET, recebe HTML
    html = send_get_request(URL, "html")
    # Salva o HTML cru do corpo
    body_html = string_cleanup(html.body)

    html = node_cleanup(html)
    # Usa .getText() para pegar o conteúdo textual da página toda
    full_text = string_cleanup(html.body.get_text(separator='\n', strip=True))

    return {
        "id": id,
        "title": title,
        "text": full_text,
        "html": body_html
    }


def upload_results(results_list: List[dict], dataset: str):
    main_df = pd.DataFrame()
    current_datetime = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    for result in results_list:
        result["_extracted_at"] = current_datetime
        single_df = pd.DataFrame(result)
        main_df = pd.concat([main_df, single_df])

    print(main_df)
    # Chamando a task de upload
    # upload_df_to_datalake.run(
    #     df=main_df,
    #     table_id="diarios",
    #     dataset_id=dataset_name,
    #     partition_column="_extracted_at",
    # )
