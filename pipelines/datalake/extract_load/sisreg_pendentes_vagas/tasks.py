# -*- coding: utf-8 -*-
"""
Tasks
"""
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
    EXTRACTION_DATE_COLUMN
)


def _verify_text(text: str, page_text: str) -> bool:
    return True if text in page_text else False


def _create_elasticsearch_client(user: str, password: str) -> Elasticsearch:
    return Elasticsearch(
        hosts=[{
            "host": ELASTIC_HOST,
            "port": ELASTIC_PORT,
            "scheme": ELASTIC_SCHEME,
        }],
        http_auth=(user, password),
        timeout=600,
        max_retries=5,
        retry_on_timeout=True,
        http_compress=True,
    )


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_procedimentos(
    user: str,
    password: str,
    max_es_pages: int = None
) -> pd.DataFrame:

    es = _create_elasticsearch_client(user=user, password=password)

    procedimentos_com_grupo = {}
    procedimentos_sem_grupo = {}

    response = es.search(index=ELASTIC_INDEX, body=DEFAULT_QUERY)
    es_page_iterator = 0

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

            if item["codigo_grupo_procedimento"]:
                procedimentos_com_grupo.setdefault(
                    item["codigo_grupo_procedimento"], item
                )
            else:
                procedimentos_sem_grupo.setdefault(
                    item["codigo_interno"], item
                )

        search_after = response["hits"]["hits"][-1]["sort"]

        response = es.search(
            index=ELASTIC_INDEX,
            body={**DEFAULT_QUERY, "search_after": search_after},
        )

    df = pd.DataFrame(
        list(procedimentos_com_grupo.values())
        + list(procedimentos_sem_grupo.values())
    )

    df = df[
        ~df["descricao_interna"]
        .fillna("")
        .str.upper()
        .str.strip()
        .str.contains(r"[-–]\s*PPI$", regex=True)
    ].reset_index(drop=True)

    log(f"{len(df)} procedimentos encontrados.")

    return df


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def extract_vagas_info(
    user: str,
    password: str,
    df_procedimentos: pd.DataFrame,
    request_delay: float,
    max_procedimentos: int = None
) -> tuple[pd.DataFrame, pd.DataFrame]:

    session = requests.Session()
    session.headers.update(SESSION_HEADERS)

    password_256 = sha256(password.upper().encode('utf-8')).hexdigest()

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

    resultados = []
    vagas_detalhadas = []

    try:
        for idx, df_row in df_procedimentos.iterrows():
            if max_procedimentos is not None and idx >= max_procedimentos:
                log(f"Limite de procedimentos atingido: {max_procedimentos}")
                break

            codigo_interno = str(df_row["codigo_interno"])
            nome_proced = df_row["descricao_interna"]
            codigo_grupo = df_row["codigo_grupo_procedimento"]

            log(f"Processando {idx}, {codigo_interno}, {nome_proced}")

            params = {
                "ETAPA": "LISTAR",
                "programa": "AUTORIZADOR",
                "radio_codProc": "interno",
                "qtd_itens_pag": 10,
                "pg": 0,
                "tplaudo": 0,
                "ordem": 2
            }

            if pd.notna(codigo_grupo):
                params["no_procedimento"] = nome_proced
            else:
                params["nu_procedimento"] = codigo_interno

            time.sleep(request_delay)

            resp = session.get(AUTORIZADOR_URL, params=params)
            resp.raise_for_status()

            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            soup_page_text = soup.get_text()

            if _verify_text("INEXISTENTES!", soup_page_text):
                resultados.append({
                    "cod_interno_proced": codigo_interno,
                    "nm_proced": nome_proced,
                    "cnes_unidade": None,
                    "nome_unidade": None,
                    "qtd_pend": None,
                    "qtd_vagas": None,
                    EXTRACTION_DATE_COLUMN: datetime.now().strftime("%Y-%m-%d"),
                    "dt_hr_extracao": datetime.now(),
                })
                continue

            titulo = soup.find_all("td", class_="td_titulo_tabela")[2]

            qtd_sol = int(re.findall(r"\d+", titulo.text)[0])

            codigo_solicitacao = (
                soup.find("form")
                .find_all("table")[2]
                .find_all("tr")[2]
                .find_all("td")[1]
                .text
            )

            time.sleep(request_delay)

            params_aplicar = {
                "ETAPA": "APLICAR",
                "tipo": "R",
                "status": "A",
                "co_cid": "H539",
                "co_solicitacao": codigo_solicitacao,
                "nu_classificacao_risco": 1,
                "radio_codProc": "interno"
            }

            resp = session.get(AUTORIZADOR_URL, params=params_aplicar)
            resp.raise_for_status()

            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            soup_page_text = soup.get_text()

            log(f"codigo_solicitacao: {codigo_solicitacao}, url: {resp.url}")

            if (
                _verify_text("NENHUMA VAGA ENCONTRADA", soup_page_text)
                or _verify_text("encontra-se autorizada.", soup_page_text)
            ):
                resultados.append({
                    "cod_interno_proced": codigo_interno,
                    "nm_proced": nome_proced,
                    "cnes_unidade": None,
                    "nome_unidade": None,
                    "qtd_pend": qtd_sol,
                    "qtd_vagas": 0,
                    EXTRACTION_DATE_COLUMN: datetime.now().strftime("%Y-%m-%d"),
                    "dt_hr_extracao": datetime.now(),
                })
                continue

            unidades = soup.select(".td_titulo_tabela")

            for k, unidade in enumerate(unidades):

                unidade_nome = unidade.text.strip().split(" - ")[0]

                div = soup.find("div", id=f"divUnidade{k}")

                if not div:
                    continue

                input_tag = div.find("input")

                if not input_tag:
                    continue

                unidade_cnes = input_tag["value"].split("|")[3]

                rows = div.find_all("td")

                for row in rows:
                    text = row.get_text(strip=True)

                    if "-" not in text:
                        continue

                    row_data = [x.strip() for x in text.split("-")]

                    if len(row_data) < 6:
                        continue

                    vaga_info = {
                        "cod_interno_proced": codigo_interno,
                        "nm_proced": nome_proced,
                        "cnes_unidade": unidade_cnes,
                        "nome_unidade": unidade_nome,
                        "data_vaga": row_data[0],
                        "dia_semana": row_data[1],
                        "hora_vaga": row_data[2],
                        "nome_profissional": row_data[3],
                        "tipo": row_data[4],
                        "qtd_vagas": row_data[5].replace("Vagas:", "").strip(),
                        EXTRACTION_DATE_COLUMN: datetime.now().strftime("%Y-%m-%d"),
                        "dt_hr_extracao": datetime.now(),
                    }

                    vagas_detalhadas.append(vaga_info)

                vagas = [
                    int(b_tag.text)
                    for b_tag in div.find_all("b")
                    if b_tag.text.isnumeric()
                ]

                qtd_vagas = sum(vagas)

                resultados.append({
                    "cod_interno_proced": codigo_interno,
                    "nm_proced": nome_proced,
                    "cnes_unidade": unidade_cnes,
                    "nome_unidade": unidade_nome,
                    "qtd_pend": qtd_sol,
                    "qtd_vagas": qtd_vagas,
                    EXTRACTION_DATE_COLUMN: datetime.now().strftime("%Y-%m-%d"),
                    "dt_hr_extracao": datetime.now()
                })

                log(f"\t ---> {k}, {unidade_nome}, {unidade_cnes}, {qtd_vagas}")

            time.sleep(request_delay)

    finally:
        session.close()

    df_dados_gerais = pd.DataFrame(resultados)

    if vagas_detalhadas:
        df_vagas_detalhes = pd.DataFrame(vagas_detalhadas)
    else:
        log("Nenhuma vaga detalhada foi extraída. Retornando DataFrame vazio com schema correto.")

        df_vagas_detalhes = pd.DataFrame(columns=[
            "cod_interno_proced",
            "nm_proced",
            "cnes_unidade",
            "nome_unidade",
            "data_vaga",
            "dia_semana",
            "hora_vaga",
            "nome_profissional",
            "tipo",
            "qtd_vagas",
            EXTRACTION_DATE_COLUMN,
            "dt_hr_extracao"
        ])

        df_vagas_detalhes = df_vagas_detalhes.astype({
            "cod_interno_proced": "object",
            "nm_proced": "object",
            "cnes_unidade": "object",
            "nome_unidade": "object",
            "data_vaga": "object",
            "dia_semana": "object",
            "hora_vaga": "object",
            "nome_profissional": "object",
            "tipo": "object",
            "qtd_vagas": "object",
            EXTRACTION_DATE_COLUMN: "object",
            "dt_hr_extracao": "datetime64[ns]"
        })

    return (df_dados_gerais, df_vagas_detalhes)
