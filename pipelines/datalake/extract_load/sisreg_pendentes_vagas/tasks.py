# -*- coding: utf-8 -*-
"""
This file has the two big jobs of our pipeline:

STEP 1 ─ "get_procedimentos"
    Ask Elasticsearch: "which medical procedures exist?"
    Returns a list of procedures and their groups.

STEP 2 ─ "extract_vagas_info"
    For EACH procedure from step 1, log into the SISREG website and:
    1. LISTAR  → how many people are waiting in line
    2. APLICAR → check if there are actual vacancies
       If YES → collect every appointment slot (date, time, doctor, unit)
       If NO  → mark "zero vacancies"

Output: dados_gerais.csv (summary) + vagas_detalhadas.csv (details)
"""

# ============================================================
# IMPORTS
# ============================================================

import re
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from hashlib import sha256
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

from prefect import task
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.datalake.extract_load.sisreg_pendentes_vagas.constants import (
    ELASTIC_INDEX,
    ELASTIC_HOST,
    ELASTIC_PORT,
    ELASTIC_SCHEME,
    LOGIN_URL,
    AUTORIZADOR_URL,
    DEFAULT_QUERY,
    SESSION_HEADERS,
    EXTRACTION_DATE_COLUMN,
    VAGAS_DETALHADAS_COLUMNS,
)

# ============================================================
# TINY HELPERS
# ============================================================


def _verify_text(text: str, page_text: str) -> bool:
    """Check if a word lives inside a page of text.

    Used to spot messages like "INEXISTENTES!" or
    "NENHUMA VAGA ENCONTRADA" on the SISREG page.
    """
    return text in page_text


def _create_elasticsearch_client(user: str, password: str) -> Elasticsearch:
    """Build a client that talks to SISREG's Elasticsearch index.

    We need this to search for procedures in step 1.
    """
    return Elasticsearch(
        hosts=[
            {
                "host": ELASTIC_HOST,
                "port": ELASTIC_PORT,
                "scheme": ELASTIC_SCHEME,
            }
        ],
        http_auth=(user, password),
        timeout=600,
        max_retries=5,
        retry_on_timeout=True,
        http_compress=True,
    )


# ============================================================
# STAGE 1 ─ FETCH PROCEDURES FROM ELASTICSEARCH
# ============================================================
#
# Before we can check vacancies, we need to know which
# procedures exist. We pull them from SISREG's Elasticsearch.


def _fetch_procedimentos_from_es(es: Elasticsearch, max_es_pages: int = None) -> tuple[dict, dict]:
    """Scroll through ES results page by page, collecting procedures.

    Splits procedures into two buckets:
      - procedimentos_com_grupo → has a group code, keyed by (group, code)
      - procedimentos_sem_grupo → no group, keyed by code only

    Each procedure appears only once (deduplicated via setdefault).
    """

    procedimentos_com_grupo = {}
    procedimentos_sem_grupo = {}

    response = es.search(index=ELASTIC_INDEX, body=DEFAULT_QUERY)
    es_page_iterator = 0

    # Keep asking for the next page until there are no more results
    while response["hits"]["hits"]:
        if max_es_pages is not None and es_page_iterator >= max_es_pages:
            log(f"Limite de páginas atingido: {max_es_pages}")
            break

        es_page_iterator += 1

        for hit in response["hits"]["hits"]:
            src = hit["_source"]

            if not src.get("procedimentos"):
                continue

            procedimento = src["procedimentos"][0]

            item = {
                "codigo_grupo_procedimento": src.get("codigo_grupo_procedimento"),
                "nome_grupo_procedimento": src.get("nome_grupo_procedimento"),
                "codigo_interno": procedimento.get("codigo_interno"),
                "descricao_interna": procedimento.get("descricao_interna"),
            }

            # Use (group, code) as dedup key when a group exists; otherwise just the code
            if item["codigo_grupo_procedimento"]:
                key = (
                    item["codigo_grupo_procedimento"],
                    item["codigo_interno"],
                )
                procedimentos_com_grupo.setdefault(key, item)
            else:
                procedimentos_sem_grupo.setdefault(item["codigo_interno"], item)

        # Ask for the NEXT page, starting after the last hit we saw
        search_after = response["hits"]["hits"][-1]["sort"]
        response = es.search(
            index=ELASTIC_INDEX,
            body={**DEFAULT_QUERY, "search_after": search_after},
        )

    return procedimentos_com_grupo, procedimentos_sem_grupo


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_procedimentos(user: str, password: str, max_es_pages: int = None) -> pd.DataFrame:
    """STEP 1: Fetch the procedure list from Elasticsearch.

    Queries SISREG's ES index for procedures matching our filter,
    deduplicates them, removes entries whose description ends with
    "PPI", and returns a DataFrame. Saves a copy to procedimentos.csv.
    """

    es = _create_elasticsearch_client(user=user, password=password)
    procedimentos_com_grupo, procedimentos_sem_grupo = _fetch_procedimentos_from_es(
        es, max_es_pages
    )

    df = pd.DataFrame(
        list(procedimentos_com_grupo.values()) + list(procedimentos_sem_grupo.values())
    )

    # Remove procedures whose description ends with "- PPI" or "PPI"
    df = df[
        ~df["descricao_interna"]
        .str.upper()
        .str.strip()
        .str.contains(r"(?:[-–]\s*)?PPI\s*$", regex=True)
    ].reset_index(drop=True)

    log(f"{len(df)} procedimentos encontrados.")
    # df.to_csv("procedimentos.csv", index=False)
    return df


# ============================================================
# STAGE 2 ─ QUERY VACANCIES ON THE SISREG WEBSITE
# ============================================================
#
# For each procedure found in stage 1, we log into the SISREG
# website and run two steps: LISTAR (get queue info) and APLICAR
# (check for actual vacancies).


def _login_sisreg(
    session: requests.Session, user: str, password: str, request_delay: float
) -> None:
    """Log into the SISREG website.

    The password is scrambled with SHA256 first (that's what
    the website expects). Raises an error if login fails.
    """
    # Scramble the password with SHA256 (the website expects this)
    password_256 = sha256(password.upper().encode("utf-8")).hexdigest()
    payload_login = {
        "usuario": user,
        "senha_256": password_256,
        "etapa": "ACESSO",
    }
    resp = session.post(LOGIN_URL, data=payload_login)
    resp.raise_for_status()
    if "Efetue o logon novamente" in resp.text:
        raise Exception("Erro no login. Verifique as credenciais ou o site.")
    time.sleep(request_delay)


def _list_procedimento(
    session: requests.Session, codigo_interno: str, request_delay: float
) -> tuple[int | None, str | None]:
    """Step "LISTAR": ask SISREG how many people are waiting/in queue for a procedure.

    Returns (qtd_sol, codigo_solicitacao) so we can use the code
    in the APLICAR step. If the procedure doesn't exist on SISREG,
    returns (None, None) to tell the caller to skip it.
    """
    time.sleep(request_delay)
    params = {
        "ETAPA": "LISTAR",
        "programa": "AUTORIZADOR",
        "radio_codProc": "interno",
        "qtd_itens_pag": 10,
        "pg": 0,
        "tplaudo": 0,
        "ordem": 2,
        "nu_procedimento": codigo_interno,
    }
    resp = session.get(AUTORIZADOR_URL, params=params)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # If the page says "INEXISTENTES!", this procedure has no data on SISREG
    if "INEXISTENTES!" in soup.get_text():
        return None, None

    # Pull the number of waiting people from the page title
    titulo = soup.find_all("td", class_="td_titulo_tabela")[2]
    qtd_sol = int(re.findall(r"\d+", titulo.text)[0])
    # Dig into the HTML soup to find the solicitation code
    codigo_solicitacao = (
        soup.find("form").find_all("table")[2].find_all("tr")[2].find_all("td")[1].text
    )
    return qtd_sol, codigo_solicitacao


def _parse_unidades_from_soup(
    soup: BeautifulSoup,
    base_record: dict,
    qtd_sol: int,
    resultados: list,
    vagas_detalhadas: list,
) -> None:
    """Read the APLICAR response page and pull out every unit's vacancies.

    For each health unit on the page:
      1. Collects every appointment slot (date, time, doctor) → vagas_detalhadas
      2. Sums up total slots per unit                     → resultados
    """

    unidades = soup.select(".td_titulo_tabela")
    for k, unidade in enumerate(unidades):
        # Grab the unit name from the table header
        unidade_nome = unidade.text.strip().split(" - ")[0]
        div = soup.find("div", id=f"divUnidade{k}")
        if not div:
            continue
        input_tag = div.find("input")
        if not input_tag:
            continue
        # Dig into the hidden input to get the unit's CNES number
        unidade_cnes = input_tag["value"].split("|")[3]

        # Each <td> row inside the unit's div is one appointment slot
        for row in div.find_all("td"):
            text = row.get_text(strip=True)
            if "-" not in text:
                continue
            row_data = [x.strip() for x in text.split("-")] # avaliar matthews
            if len(row_data) < 6:
                continue
            vagas_detalhadas.append(
                {
                    **base_record,
                    "cnes_unidade": unidade_cnes,
                    "nome_unidade": unidade_nome,
                    "data_vaga": row_data[0],
                    "dia_semana": row_data[1],
                    "hora_vaga": row_data[2],
                    "nome_profissional": row_data[3],
                    "tipo": row_data[4],
                    "qtd_vagas": row_data[5].replace("Vagas:", "").strip(),
                }
            )

        # Count the bold (<b>) numbers — that's the total slots per unit
        vagas = [int(b_tag.text) for b_tag in div.find_all("b") if b_tag.text.isnumeric()]
        qtd_vagas = sum(vagas)
        resultados.append(
            {
                **base_record,
                "cnes_unidade": unidade_cnes,
                "nome_unidade": unidade_nome,
                "qtd_pend": qtd_sol,
                "qtd_vagas": qtd_vagas,
            }
        )
        log(f"\t ---> {k}, {unidade_nome}, {unidade_cnes}, {qtd_vagas}")


def _process_vagas_apply(
    session: requests.Session,
    codigo_solicitacao: str,
    request_delay: float,
    base_record: dict,
    qtd_sol: int,
    resultados: list,
    vagas_detalhadas: list,
) -> None:
    """Step "APLICAR": check if there are actual vacancies for this procedure.

    Two possible answers from SISREG:
      - "NENHUMA VAGA ENCONTRADA" → no slots at all
      - "encontra-se autorizada"  → already booked
    In either case, we record 0 vacancies and stop.

    If neither message appears, the page has vacancies → we call
    _parse_unidades_from_soup to read all the slot details.
    """
    time.sleep(request_delay)
    params_aplicar = {
        "ETAPA": "APLICAR",
        "tipo": "R",
        "status": "A",
        "co_cid": "H539",
        "co_solicitacao": codigo_solicitacao,
        "nu_classificacao_risco": 1,
        "radio_codProc": "interno",
    }
    resp = session.get(AUTORIZADOR_URL, params=params_aplicar)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    soup_page_text = soup.get_text()

    log(f"codigo_solicitacao: {codigo_solicitacao}, url: {resp.url}")

    # Website says "no vacancy found" or "already authorized"
    if _verify_text("NENHUMA VAGA ENCONTRADA", soup_page_text) or _verify_text(
        "encontra-se autorizada.", soup_page_text
    ):
        resultados.append(
            {
                **base_record,
                "cnes_unidade": None,
                "nome_unidade": None,
                "qtd_pend": qtd_sol,
                "qtd_vagas": 0,
            }
        )
        return

    _parse_unidades_from_soup(soup, base_record, qtd_sol, resultados, vagas_detalhadas)


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def extract_vagas_info(
    user: str,
    password: str,
    df_procedimentos: pd.DataFrame,
    request_delay: float,
    max_procedimentos: int = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """STEP 2: Query vacancies for every procedure found in step 1.

    Logs into SISREG, then for each procedure:
      1. LISTAR  → get queue size + solicitation code
         ↳ procedure missing    → record "no data", skip
      2. APLICAR → check for actual vacancies
         ↳ none found           → record 0, skip
         ↳ has vacancies        → collect date/time/doctor for each slot

    Returns (df_dados_gerais, df_vagas_detalhes) and saves CSV copies.
    """

    # creates session, logs in, allowing proper use forward in the code. Closes by the end.
    session = requests.Session()
    session.headers.update(SESSION_HEADERS)
    _login_sisreg(session, user, password, request_delay)

    resultados = []
    vagas_detalhadas = []
    extraction_dt = datetime.now()
    extraction_date = extraction_dt.strftime("%Y-%m-%d")

    try:
        for idx, df_row in df_procedimentos.iterrows():

            if max_procedimentos is not None and idx >= max_procedimentos:
                log(f"Limite de procedimentos atingido: {max_procedimentos}")
                break

            codigo_interno = str(df_row["codigo_interno"])
            nome_proced = df_row["descricao_interna"]
            codigo_grupo = df_row.get("codigo_grupo_procedimento")
            nome_grupo = df_row.get("nome_grupo_procedimento")
            is_grupo = pd.notna(codigo_grupo)

            log(f"Processando {idx}, {codigo_interno}, {nome_proced}")

            # Fields that stay the same across all records for this procedure
            base_record = {
                "codigo_grupo_procedimento": codigo_grupo,
                "nome_grupo_procedimento": nome_grupo,
                "is_grupo": is_grupo,
                "cod_interno_proced": codigo_interno,
                "nm_proced": nome_proced,
                EXTRACTION_DATE_COLUMN: extraction_date,
                "dt_hr_extracao": extraction_dt,
            }

            # STEP 2a: LISTAR — ask how many people are waiting
            qtd_sol, codigo_solicitacao = _list_procedimento(session, codigo_interno, request_delay)

            # Procedure doesn't exist on SISREG → record nulls and move on
            if qtd_sol is None:
                resultados.append(
                    {
                        **base_record,
                        "cnes_unidade": None,
                        "nome_unidade": None,
                        "qtd_pend": None,
                        "qtd_vagas": None,
                    }
                )
                continue

            # STEP 2b: APLICAR — check for actual vacancies
            _process_vagas_apply(
                session,
                codigo_solicitacao,
                request_delay,
                base_record,
                qtd_sol,
                resultados,
                vagas_detalhadas,
            )

            time.sleep(request_delay)

    finally:
        session.close()

    # If we never found detailed slots, return an empty DataFrame with the right columns
    df_dados_gerais = pd.DataFrame(resultados)
    if vagas_detalhadas:
        df_vagas_detalhes = pd.DataFrame(vagas_detalhadas)
    else:
        log("Nenhuma vaga detalhada foi extraída. Retornando DataFrame vazio com schema correto.")
        df_vagas_detalhes = pd.DataFrame(columns=VAGAS_DETALHADAS_COLUMNS)

    # df_dados_gerais.to_csv("dados_gerais.csv", index=False)
    # df_vagas_detalhes.to_csv("vagas_detalhadas.csv", index=False)

    return (df_dados_gerais, df_vagas_detalhes)
