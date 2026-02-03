# -*- coding: utf-8 -*-
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
import pytz
from google.cloud import bigquery
from selenium import webdriver
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_uniao.utils import (
    extract_decree_details,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake
from pipelines.utils.time import parse_date_or_today

# Configurações do Web Driver
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-plugins")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-images")
chrome_options.add_argument("--page-load-strategy=eager")


@task(nout=2)
def dou_extraction(dou_section: int, max_workers: int, date: datetime) -> list:
    """Tarefa para extração dos dados de atos oficiais do DOU.

    Args:
        dou_section (int): Seção do DOU a ser extraída.
        max_workers (int): Número máximo de workers para execução paralela.
        date (datetime): Data do diário oficial a ser extraído.

    Raises:
        exc: Máximo de tentativas alcançado na requisição de algum ato oficial.

    Returns:
        list: Lista de dicionários contendo os dados extraídos de cada ato do DOU e a variável que indica que a extração foi bem sucedida. # noqa

    """
    date = parse_date_or_today(date)

    items = []  # Lista de dicionários com os metadados e informações de cada ato
    page_count = 1

    day, month, year = date.day, date.month, date.year

    driver = webdriver.Chrome(options=chrome_options)
    log("🤖 Iniciando o webdriver...")
    log(
        f"Iniciando extração dos atos oficiais do DOU Seção {str(dou_section)} de {date.strftime('%d/%m/%Y')}"  # noqa
    )

    try:
        driver.get(
            f"https://www.in.gov.br/leiturajornal?data={day}-{month}-{year}&secao=do{str(dou_section)}"  # noqa
        )
    except WebDriverException:
        log("❌ Erro ao acessar o site do DOU.")
        return [[], False]

    while True:
        log(f"Extração da página {page_count}")

        # Quando não há atos oficias, há um elemento de aviso
        if driver.find_elements(by=By.CLASS_NAME, value="alert.alert-info"):
            log("⚠️ Não há atos oficias para extrair.")
            break

        cards = driver.find_elements(by=By.CLASS_NAME, value="resultado")

        decree_links_to_process = []  # Lista para armazenar os links dos decretos

        # Pegando a url de cada ato na página de pesquisa
        for card in cards:
            text_link = card.find_element(by=By.TAG_NAME, value="a")
            decree_links_to_process.append(text_link.get_attribute("href"))

        # Implementando extracao com concorrência
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {
                executor.submit(extract_decree_details, url): url for url in decree_links_to_process
            }

            for i, future in enumerate(as_completed(future_to_url)):
                url = future_to_url[future]
                try:
                    decree_details = future.result()
                    items.append(decree_details)
                except Exception:
                    log(
                        f"❌ A requisição para {url} alcançou o máximo de tentativas na requisição."
                    )
                    return [[], False]

        # Buscando o botão para a próxima página de pesquisa
        pagination_buttons = driver.find_elements(by=By.CLASS_NAME, value="pagination-button")
        next_btn = None

        if pagination_buttons:
            for button in pagination_buttons:
                if "Próximo" in button.text:
                    next_btn = button

        if next_btn:
            next_btn.click()
            page_count += 1
            time.sleep(0.5)
        else:
            break  # Não há o botão para a próxima paǵina. Chegou na última página da seção, fim da extração # noqa

    driver.quit()
    log("✅ Extração finalizada.")
    return [items, True]


@task
def upload_to_datalake(dou_infos: list, dataset: str) -> None:
    """Função para realizar a carga dos dados extraídos no datalake da SMS Rio.

    Args:
        dou_infos (dict): Lista com os dicionários contendo os dados extraídos de cada ato do DOU.
        dataset (str): Dataset do BigQuery onde os dados serão carregados.
        successful (bool): Variável que indica que a extração foi bem sucedida.
        environment (str):
    """

    if dou_infos:
        extracted_at = datetime.now(pytz.timezone("America/Sao_Paulo")).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        rows = len(dou_infos)
        df = pd.DataFrame(dou_infos)
        df["extracted_at"] = extracted_at
        log(f"Realizando upload de {rows} registros no datalake em {dataset}...")

        upload_df_to_datalake.run(
            df=df,
            dataset_id=dataset,
            table_id="diarios_uniao",
            partition_column="extracted_at",
            if_exists="append",
            if_storage_data_exists="append",
            source_format="csv",
            csv_delimiter=",",
        )
        log("✅ Carregamento no datalake finalizado.")
    else:
        log("❌ Não há dados para carregar.")


@task
def report_extraction_status(status: bool, date: str, dou_section: int, environment: str = "dev"):

    date = parse_date_or_today(date).strftime("%Y-%m-%d")

    success = "true" if status else "false"
    current_datetime = datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
    tipo_diario = "dou-sec" + str(dou_section)

    if environment is None:
        environment = "dev"

    PROJECT = constants.GOOGLE_CLOUD_PROJECT.value[environment]
    DATASET = "projeto_cdi"
    TABLE = "extracao_status"
    FULL_TABLE_NAME = f"`{PROJECT}.{DATASET}.{TABLE}`"

    log(f"Inserting into {FULL_TABLE_NAME} status of success={success} for date='{date}'...")

    client = bigquery.Client()
    query_job = client.query(f"""
        INSERT INTO {FULL_TABLE_NAME} (
            data_publicacao, tipo_diario, extracao_sucesso, _updated_at
        )
        VALUES (
            '{date}', '{tipo_diario}', {success}, '{current_datetime}'
        )
    """)
    query_job.result()
    log("✅ Status da extração reportado!")
