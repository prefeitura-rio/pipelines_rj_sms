# -*- coding: utf-8 -*-
"""
Tasks
"""
# Geral
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from hashlib import sha256
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log
from requests import Session
from unidecode import unidecode
from random import randint
from time import sleep

# Internos
from pipelines.datalake.extract_load.sisreg_afastamentos import constants
from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_extraction_date() -> datetime:
    return datetime.now(ZoneInfo("America/Sao_Paulo"))


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_cpf_profissionais(
    environment: str,
    extraction_date: datetime,
    sample: int = 1,
    slice: int = 10,
    initial_date_offset: int = 30,
) -> list[str]:
    procedimento_vigencia_inicial_data = (
        extraction_date.date()
        - timedelta(initial_date_offset)
    ).strftime("%Y-%m-%d")

    client = bigquery.Client()
    log("Cliente Big Query OK")

    if sample < 1 or sample > 100:
        sample = None
        log("Invalid sample, querying all data")

    sql_sample = (
        "WHERE procedimento_vigencia_inicial_data <= "
        f"\"{procedimento_vigencia_inicial_data}\""
        if environment != "dev" or sample is None
        else f"TABLESAMPLE SYSTEM ({sample} PERCENT) LIMIT {slice}"
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
def init_session_request_base() -> requests.Session:
    session = requests.session()

    session.get(
        "https://sisregiii.saude.gov.br/",
        headers=constants.REQUEST_HEADERS,
    )

    return session


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def login_sisreg(usuario: str, senha: str, session: Session) -> Session:
    payload = (
        f"usuario={usuario}&"
        "senha=&"
        f"senha_256={sha256(senha.upper().encode('utf-8')).hexdigest()}&"
        "etapa=ACESSO&"
        "logout="
    )

    log("Realizando autenticação")
    response = session.post(
        "https://sisregiii.saude.gov.br/",
        headers=constants.REQUEST_HEADERS | {
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=payload,
        allow_redirects=True,
    )

    if usuario not in response.text:
        log("Não logado")
        raise Exception("Not logged in")

    log("Login com sucesso")
    return session


@task(max_retries=1, retry_delay=timedelta(minutes=5), )
def get_afastamentos(
    session: Session,
    cpf_list: list[str],
    extraction_date: datetime,
    sleep_time: int,
    historico: bool,
    sleep_time_window: int = 4,
) -> pd.DataFrame:
    log("iniciando extração dos afastamentos")
    df = None
    for cpf in cpf_list:
        tempo_espera_sorteado = randint(
            sleep_time,
            sleep_time+sleep_time_window
        )
        sleep(tempo_espera_sorteado)
        if historico:
            ret = search_historico_afastamento(
                cpf,
                session,
                extraction_date,
            )
        else:
            ret = search_afastamento(
                cpf,
                session,
                extraction_date,
            )

        if df is None:
            df = ret
        elif ret is not None:
            df = pd.concat((df, ret))

    return df


# @task(max_retries=1, retry_delay=timedelta(minutes=5), )
# def get_historico_afastamentos(
#     session: Session,
#     cpf_list: list[str],
#     extraction_date: datetime,
#     sleep_time: int,
#     sleep_time_window: int = 4,
# ) -> pd.DataFrame:
#     df = None
#     for cpf in cpf_list:
#         sleep(randint(sleep_time, sleep_time+sleep_time_window))
#         ret = search_historico_afastamento(
#             cpf,
#             session,
#             extraction_date,
#         )
#         if df is None:
#             df = ret
#         elif ret is not None:
#             df = pd.concat((df, ret))
#
#     return df


def search_afastamento(
    cpf: str,
    session: Session,
    extraction_date: datetime,
) -> pd.DataFrame | None:
    res = session.get(
        f"https://sisregiii.saude.gov.br/cgi-bin/af_medicos.pl?cpf={cpf}&mostrar_antigos=1",
        headers=constants.REQUEST_HEADERS,
    )

    if "Profissional nao encontrado!" in res.text:
        log(f"Profissional de cpf '{cpf}' não encontrado")
        return None
    if "(CAPTCHA)" in res.text:
        raise Exception("SISREG está pedindo CAPTCHA")

    html_tables = BeautifulSoup(
        res.content, "lxml").find_all(class_="table_listagem")

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
    df["data_fim"] = df["data_fim"].apply(
        lambda value: datetime.strptime(value, "%d/%m/%Y").date())
    df["hora_inicio"] = df["hora_inicio"].apply(
        lambda value: datetime.strptime(value, "%H:%M").time()
    )
    df["hora_fim"] = df["hora_fim"].apply(
        lambda value: datetime.strptime(value, "%H:%M").time())
    df["cpf"] = cpf
    df[constants.EXTRACTION_DATE_COLUMN] = extraction_date

    return df


def search_historico_afastamento(
    cpf: str,
    session: Session,
    extraction_date: datetime,
) -> pd.DataFrame | None:
    res = session.get(
        "https://sisregiii.saude.gov.br/cgi-bin/af_medicos.pl?"
        f"cpf={cpf}&op=Log",
        headers=constants.REQUEST_HEADERS,
    )

    if "Nenhum Log Encontrado" in res.text:
        log(f"Nenhum log encontrado para o CPF {cpf}")
        return None

    if "(CAPTCHA)" in res.text:
        raise Exception("SISREG está pedindo CAPTCHA")

    df = pd.read_html(
        StringIO(BeautifulSoup(res.content, "lxml").find(
            class_="table_listagem").__str__())
    )[0]

    df.columns = df.iloc[1]
    df = (
        df.drop([0, 1])
        .reset_index(drop=True)
        .rename(
            lambda name: (unidecode(name).replace(
                "Codigo", "codigo_afastamento").lower()),
            axis=1,
        )
    )

    df["data"] = df["data"].apply(
        lambda value: datetime.strptime(value, "%d/%m/%Y - %H:%M"))

    df["cpf"] = cpf
    df[constants.EXTRACTION_DATE_COLUMN] = extraction_date

    return df


@task
def log_df(df: pd.DataFrame, name: str):
    log(f"{name}:\n{df}")
