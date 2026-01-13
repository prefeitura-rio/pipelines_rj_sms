# -*- coding: utf-8 -*-
"""
Tasks
"""
# Geral
from datetime import datetime, timedelta
from hashlib import sha256
from io import StringIO

import pandas as pd
import requests
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
def get_cpf_profissionais(environment, limit=10) -> pd.DataFrame:
    client = bigquery.Client()
    log("Cliente Big Query OK")

    sql_limit = "" if environment != "dev" or limit is None else f"LIMIT {limit}"
    sql = f"""
        SELECT distinct
           cpf_profissional_exec
        FROM `rj-sms.brutos_sisreg_staging.escala`
        {sql_limit}
        ;
    """

    log(sql)
    df = client.query_and_wait(sql).to_dataframe()
    log("Query executada com sucesso")

    res = df["cpf_profissional_exec"].to_list()
    return res


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_base_request() -> requests.Session:
    session = requests.session()

    session.get(
        "https://sisregiii.saude.gov.br/",
        headers={
            "Connection": "keep-alive",
        },
    )

    return session


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def login_sisreg(usuario: str, senha: str, session: Session) -> Session:
    def make_payload(usuario: str, senha: str) -> str:
        return (
            f"usuario={usuario}&"
            "senha=&"
            f"senha_256={sha256(senha.upper().encode('utf-8')).hexdigest()}&"
            "etapa=ACESSO&"
            "logout="
        )

    log("Realizando autenticação")
    response = session.post(
        "https://sisregiii.saude.gov.br/",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=make_payload(usuario, senha),
        allow_redirects=True,
    )

    if usuario not in response.text:
        log("Não logado")
        raise Exception("Not logged in")

    log("Login com sucesso")
    return session


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def search_afastamentos(
    cpf: str, session: Session, extraction_date: datetime
) -> pd.DataFrame | None:
    res = session.get(
        ("https://sisregiii.saude.gov.br/cgi-bin/af_medicos.pl?" f"cpf={cpf}&mostrar_antigos=1"),
    )

    if "Profissional nao encontrado!" in res.text:
        log(f"Profissional de cpf '{cpf}' não encontrado no SISREG")
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


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def search_historico_afastamentos(
    cpf: str, session: Session, extraction_date: datetime
) -> pd.DataFrame | None:
    res = session.get(
        ("https://sisregiii.saude.gov.br/cgi-bin/af_medicos.pl?" f"cpf={cpf}&op=Log"),
    )

    if "Nenhum Log Encontrado" in res.text:
        log(f"Nenhum log encontrado para o CPF {cpf}")
        return None

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
