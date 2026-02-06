# -*- coding: utf-8 -*-
"""
Tasks
"""
# Geral
from datetime import datetime, timedelta
from hashlib import sha256
from io import StringIO

import pandas as pd
# import requests
import httpx
from bs4 import BeautifulSoup
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
from requests import Session
from unidecode import unidecode

# Internos
from pipelines.datalake.extract_load.sisreg_afastamentos import constants
from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_extraction_date() -> datetime:
    return datetime.now()


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_cpf_profissionais(environment: str, sample: int = 1, slice: int = 10) -> list[str]:
    client = bigquery.Client()
    log("Cliente Big Query OK")

    if sample < 1 or sample > 100:
        sample = None
        log("Invalid sample, querying all data")

    sql_sample = (
        "" if environment != "dev" or sample is None else f"TABLESAMPLE SYSTEM ({sample} PERCENT)"
    )
    sql = f"""
        SELECT distinct
          profissional_executante_cpf
        FROM `rj-sms.brutos_sisreg.oferta_programada`
        {sql_sample}
        ;
    """

    log(sql)
    df = client.query_and_wait(sql).to_dataframe()
    log(f"Query executada com sucesso, {len(df)} CPF retornados")

    res = df["profissional_executante_cpf"].to_list()
    if environment == "dev":
        res = res[:slice]
        log(f"CPF reduzido a {len(res)} elementos")
    return res


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def init_client_request_base() -> httpx.Client:
    client = httpx.Client()

    res = client.get(
        "https://sisregiii.saude.gov.br/",
        headers=constants.REQUEST_HEADERS,
    )

    return client


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def login_sisreg(usuario: str, senha: str, client: httpx.Client) -> httpx.Client:
    payload = (
        f"usuario={usuario}&"
        "senha=&"
        f"senha_256={sha256(senha.upper().encode('utf-8')).hexdigest()}&"
        "etapa=ACESSO&"
        "logout="
    )

    log("Realizando autenticação")
    response = client.post(
        "https://sisregiii.saude.gov.br/",
        headers=constants.REQUEST_HEADERS | {
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=payload,
        follow_redirects=True,
    )

    if usuario not in response.text:
        log("Não logado")
        raise Exception("Not logged in")

    log("Login com sucesso")
    return client


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def search_afastamentos(
    cpf: str, client: httpx.Client, extraction_date: datetime
) -> pd.DataFrame | None:
    res = client.get(
        f"https://sisregiii.saude.gov.br/cgi-bin/af_medicos.pl?cpf={cpf}&mostrar_antigos=1",
        headers=constants.REQUEST_HEADERS,
    )

    if "Profissional nao encontrado!" in res.text:
        log(f"Profissional de cpf '{cpf}' não encontrado")
        return None

    html_tables = BeautifulSoup(res.content, "lxml").find_all(class_="table_listagem")

    if len(html_tables) <= 1:
        log(f"Listagem vazia para cpf {cpf}")
        return None

    html_table = html_tables[1]

    df = pd.read_html(StringIO(str(html_table)))[0]
    df.columns = df.iloc[1]
    df = (
        df.drop([0, 1])
        .reset_index(drop=True)
        .dropna(axis=1, how="all")
        .rename(
            lambda name: (
                unidecode(name)
                .replace("Dt. ", "data_")
                .replace("Hr. ", "hora_")
                .replace("Cod", "codigo_afastamento")
                .lower()
            ),
            axis=1,
        )
    )

    df["data_inicio"] = df["data_inicio"].apply(
        lambda value: datetime.strptime(value, "%d/%m/%Y").date()
    )
    df["data_fim"] = df["data_fim"].apply(lambda value: datetime.strptime(value, "%d/%m/%Y").date())
    df["hora_inicio"] = df["hora_inicio"].apply(
        lambda value: datetime.strptime(value, "%H:%M").time()
    )
    df["hora_fim"] = df["hora_fim"].apply(lambda value: datetime.strptime(value, "%H:%M").time())
    df["cpf"] = cpf
    df[constants.EXTRACTION_DATE_COLUMN] = extraction_date

    return df


@task(max_retries=3, retry_delay=timedelta(minutes=500))
def search_historico_afastamentos(
    cpf: str, client: httpx.Client, extraction_date: datetime
) -> pd.DataFrame | None:
    res = client.get(
        ("https://sisregiii.saude.gov.br/cgi-bin/af_medicos.pl?" f"cpf={cpf}&op=Log"),
        headers=constants.REQUEST_HEADERS,
    )

    if "Nenhum Log Encontrado" in res.text:
        log(f"Nenhum log encontrado para o CPF {cpf}")
        return None

    if "(CAPTCHA)" in res.text:
        raise Exception("SISREG está pedindo CAPTCHA")

    df = pd.read_html(
        StringIO(BeautifulSoup(res.content, "lxml").find(class_="table_listagem").__str__())
    )[0]

    df.columns = df.iloc[1]
    df = (
        df.drop([0, 1])
        .reset_index(drop=True)
        .rename(
            lambda name: (unidecode(name).replace("Codigo", "codigo_afastamento").lower()),
            axis=1,
        )
    )

    df["data"] = df["data"].apply(lambda value: datetime.strptime(value, "%d/%m/%Y - %H:%M"))

    df["cpf"] = cpf
    df[constants.EXTRACTION_DATE_COLUMN] = extraction_date

    return df


@task
def concat_dfs(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(dfs)


@task
def log_df(df: pd.DataFrame, name: str):
    log(f"{name}:\n{df}")


@task
def close_httpx_client(client: httpx.Client, wait_dfs: list[pd.DataFrame]):
    """
    O wait_dfs é para garantir que todos os dados já foram buscados antes
    da execução dessa task
    """
    log("Fechando o cliente httpx criado na execução do flow")
    client.close()
