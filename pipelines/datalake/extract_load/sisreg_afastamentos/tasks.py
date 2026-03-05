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


import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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



def _host_from_url(url: str) -> str:
    return urlparse(url).hostname or ""


def _build_firefox_driver(page_load_timeout_s: int = 45) -> webdriver.Firefox:
    options = FirefoxOptions()
    #options.add_argument("-headless")

    # Mantém o user-agent consistente com o que o pipeline já usa nos headers,
    # reduzindo discrepâncias entre o login (browser) e o scraping (httpx).
    ua = constants.REQUEST_HEADERS.get("User-Agent")
    if ua:
        options.set_preference("general.useragent.override", ua)

    # Reduz variação de HTML e ajuda a estabilizar alguns fluxos.
    options.set_preference("intl.accept_languages", "pt-BR,pt,en-US,en")

    # Timeouts de rede do Firefox para evitar travamentos silenciosos.
    options.set_preference("network.http.connection-timeout", 30)
    options.set_preference("network.http.response.timeout", 30)

    driver = webdriver.Firefox(options=options)
    driver.set_page_load_timeout(page_load_timeout_s)
    return driver


def _selenium_login_and_export_state(
    usuario: str,
    senha: str,
    wait_s: int = 30,
) -> Tuple[List[dict], str]:
    """Faz login via Firefox (Selenium) e retorna (cookies, user_agent)."""
    driver = _build_firefox_driver()
    try:
        driver.get(constants.SISREG_LOGIN_URL)
        wait = WebDriverWait(driver, wait_s)

        # Tentativa mais robusta de achar os campos, sem depender de um único seletor.
        usuario_el = wait.until(
            EC.any_of(
                EC.presence_of_element_located((By.NAME, "usuario")),
                EC.presence_of_element_located((By.ID, "usuario")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[name='usuario']")),
            )
        )
        senha_el = wait.until(
            EC.any_of(
                EC.presence_of_element_located((By.NAME, "senha")),
                EC.presence_of_element_located((By.ID, "senha")),
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[type='password']")),
            )
        )

        usuario_el.clear()
        usuario_el.send_keys(usuario)
        senha_el.clear()
        senha_el.send_keys(senha)

        # Submissão: tenta botão submit; se não existir, ENTER no campo de senha.
        try:
            driver.find_element(By.CSS_SELECTOR, "button[type='submit'], input[type='submit'], input[name='entrar']").click()
        except Exception:
            senha_el.send_keys(Keys.ENTER)

        # Confirmação mínima de que a sessão foi criada: cookie SESSION.
        def _has_session_cookie(drv) -> bool:
            try:
                c = drv.get_cookie("SESSION")
                return bool(c and c.get("value"))
            except Exception:
                return False

        wait.until(_has_session_cookie)

        # Warm-up em página pós-login para estabilizar cookies de borda/WAF.
        #driver.get(constants.SISREG_POST_LOGIN_PROBE_URL)
        #time.sleep(0.8)

        cookies = driver.get_cookies()
        user_agent = driver.execute_script("return navigator.userAgent;") or ""
        return cookies, user_agent
    finally:
        driver.quit()


def _apply_selenium_cookies_to_httpx_client(client: httpx.Client, cookies: List[dict]) -> None:
    default_domain = getattr(constants, "SISREG_DEFAULT_DOMAIN", _host_from_url(constants.SISREG_BASE_URL))

    for c in cookies:
        name = c.get("name")
        value = c.get("value")
        if not name:
            continue

        domain = c.get("domain") or default_domain
        path = c.get("path") or "/"
        client.cookies.set(name, value, domain=domain, path=path)


def _login_sisreg_via_http(usuario: str, senha: str, client: httpx.Client) -> httpx.Client:
    """Fallback: login antigo via POST direto (mantido para resiliência)."""
    payload = (
        f"usuario={usuario}&"
        "senha=&"
        f"senha_256={sha256(senha.upper().encode('utf-8')).hexdigest()}&"
        "etapa=ACESSO&"
        "logout="
    )

    log("Realizando autenticação via POST (fallback)")
    response = client.post(
        constants.SISREG_BASE_URL + "/",
        headers=constants.REQUEST_HEADERS | {
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data=payload,
        follow_redirects=True,
    )

    if usuario not in response.text:
        log("Não logado (fallback)")
        raise Exception("Not logged in (fallback)")

    log("Login com sucesso (fallback)")
    return client


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def init_client_request_base() -> httpx.Client:
    # Importante: não faz request aqui, porque o cenário reportado é timeout na primeira
    # requisição HTTP a partir da nuvem. O primeiro contato “humano” fica com o Selenium.
    timeout = httpx.Timeout(connect=100.0, read=35.0, write=35.0, pool=10.0)
    limits = httpx.Limits(max_connections=20, max_keepalive_connections=10, keepalive_expiry=20.0)

    client = httpx.Client(
        headers=constants.REQUEST_HEADERS,
        follow_redirects=True,
        trust_env=False,
        timeout=timeout,
        limits=limits,
        http2=False,
    )
    return client



@task(max_retries=3, retry_delay=timedelta(minutes=5))
def login_sisreg(usuario: str, senha: str, client: httpx.Client) -> httpx.Client:
    """
    Estratégia híbrida:
    - Selenium (Firefox) realiza o login "de verdade" e obtém cookies de sessão / WAF.
    - HTTPX reaproveita esses cookies para fazer as requisições do scraping.
    """
    try:
        log("Realizando autenticação via Selenium (Firefox) para obter cookies válidos de sessão")
        cookies, user_agent = _selenium_login_and_export_state(usuario=usuario, senha=senha)

        _apply_selenium_cookies_to_httpx_client(client, cookies)

        # Mantém o UA do client alinhado com o browser (mesmo que os requests passem headers).
        if user_agent:
            client.headers["User-Agent"] = user_agent

        # Probe pós-login: falhar aqui é melhor do que passar horas raspando HTML de login.
        probe = client.get(constants.SISREG_POST_LOGIN_PROBE_URL, headers=constants.REQUEST_HEADERS)

        if "(CAPTCHA)" in probe.text:
            raise Exception("SISREG está pedindo CAPTCHA")

        # Critério “fraco” mas útil: o HTML pós-login costuma conter o usuário em algum ponto.
        if usuario not in probe.text:
            log("Login via Selenium possivelmente realizado, mas a verificação textual não confirmou o usuário")

        log("Login híbrido com sucesso")
        return client

    except Exception as e:
        # Em caso de ambiente sem Firefox/geckodriver, ou mudança na tela de login,
        # mantemos o método antigo como fallback.
        log(f"Falha no login via Selenium: {e!r}. Tentando fallback via POST.")
        return _login_sisreg_via_http(usuario=usuario, senha=senha, client=client)



@task(max_retries=3, retry_delay=timedelta(minutes=5))
def search_afastamentos(
    cpf: str, client: httpx.Client, extraction_date: datetime
) -> pd.DataFrame | None:
    res = client.get(
        f"{constants.SISREG_BASE_URL}/cgi-bin/af_medicos.pl?cpf={cpf}&mostrar_antigos=1",
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
        (f"{constants.SISREG_BASE_URL}/cgi-bin/af_medicos.pl?cpf={cpf}&op=Log"),
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
