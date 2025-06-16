# -*- coding: utf-8 -*-
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup
from prefect import task
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

from pipelines.datalake.extract_load.diario_oficial_uniao.utils import (
    extract_decree_details
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake

# Configurações do Web Driver
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--disable-plugins")
chrome_options.add_argument("--disable-extensions")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-images")
chrome_options.add_argument("--page-load-strategy=eager")


@task
def dou_extraction(dou_section: int, max_workers: int, date: datetime) -> list:
    """Tarefa para extração dos dados de atos oficiais do DOU.

    Args:
        dou_section (int): Seção do DOU a ser extraída.
        max_workers (int): Número máximo de workers para execução paralela.
        date (datetime): Data do diário oficial a ser extraído.

    Raises:
        exc: _description_

    Returns:
        list: Lista de dicionários contendo os dados extraídos de cada ato do DOU.
    """
    if not date:
        date = datetime.now()

    items = []  # Lista de dicionários com os metadados e informações de cada ato
    page_count = 1

    day, month, year = date.day, date.month, date.year

    driver = webdriver.Chrome(options=chrome_options)
    log("🤖 Iniciando o webdriver...")
    log(
        f"Iniciando extração dos atos oficiais do DOU Seção {str(dou_section)} de {date.strftime('%d/%m/%Y')}"
    )
    driver.get(
        f"https://www.in.gov.br/leiturajornal?data={day}-{month}-{year}&secao=do{str(dou_section)}"
    )
    
    while True:
        # Lógica para evitar a poluição do log
        if page_count == 1 or page_count % 10 == 0:
            log(f"Extração da página {page_count}")
        
        # Quando não há atos oficias, há um elemento de aviso
        if driver.find_elements(by=By.CLASS_NAME, value='alert.alert-info'):
            log('⚠️ Não há atos oficias para extrair.')
            break
        
        cards = driver.find_elements(by=By.CLASS_NAME, value="resultado")

        decree_links_to_process = []  # Lista para armazenar os links dos decretos
        card_metadata = (
            []
        )  # Para armazenar title e outros dados que não dependem da requisição interna
        
        for card in cards:
            title = card.find_element(by=By.CLASS_NAME, value="title-marker")
            info = card.find_element(
                by=By.CLASS_NAME, value="breadcrumb-item.publication-info-marker"
            )
            text_link = card.find_element(by=By.TAG_NAME, value="a")

            decree_links_to_process.append(text_link.get_attribute("href"))
            card_metadata.append({"title": title.text, "info": info.text})

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {
                executor.submit(extract_decree_details, url): url for url in decree_links_to_process
            }

            for i, future in enumerate(as_completed(future_to_url)):
                url = future_to_url[future]
                try:
                    decree_details = future.result()
                    # Combinar os metadados do card com os detalhes do decreto
                    combined_item = {**card_metadata[i], **decree_details}
                    items.append(combined_item)
                except Exception as exc:
                    log(f"{url} gerou uma exceção: {exc}")
                    raise exc

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
            break  # Chegou na última página da seção. Fim da extração

    driver.quit()
    log("✅ Extração finalizada.")

    return items


@task
def upload_to_datalake(dou_infos: dict, dataset: str, environment: str) -> None:
    """Função para realizar a carga dos dados extraídos no datalake.

    Args:
        dou_infos (dict): Lista com os dicionários contendo os dados extraídos de cada ato do DOU.
        dataset (str): Dataset do BigQuery onde os dados serão carregados.
        environment (str):
    """
    
    if dou_infos:
        rows  = len(dou_infos)
        df = pd.DataFrame(dou_infos)
        df["extracted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
def parse_date(date_string: str) -> datetime | None:
    if date_string != '':
        return datetime.strptime(date_string, "%d/%m/%Y")
    return None
