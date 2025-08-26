# -*- coding: utf-8 -*-
import datetime
import os
import shutil
import time
import zipfile

import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_uniao_api.constants import (
    constants as flow_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake
from pipelines.utils.time import parse_date_or_today


@task
def create_dirs():
    if not os.path.exists(flow_constants.DOWNLOAD_DIR.value):
        os.makedirs(flow_constants.DOWNLOAD_DIR.value)
    if not os.path.exists(flow_constants.OUTPUT_DIR.value):
        os.makedirs(flow_constants.OUTPUT_DIR.value)


@task
def parse_date(date: str) -> datetime:
    return parse_date_or_today(date)


@task
def login(enviroment: str = "dev"):

    password = get_secret_key.run(
        secret_path=flow_constants.INFISICAL_PATH.value,
        secret_name=flow_constants.INFISICAL_PASSWORD.value,
        environment=enviroment,
    )
    email = get_secret_key.run(
        secret_path=flow_constants.INFISICAL_PATH.value,
        secret_name=flow_constants.INFISICAL_USERNAME.value,
        environment=enviroment,
    )

    """Inicia uma sessão e faz o login no sistema da INLABS para obter os cookies.

    Returns:
        requests.Session: Instância de Session da biblioteca requests contém os cookies da sessão.
    """
    login_url = flow_constants.LOGIN_URL.value

    payload = {"email": email, "password": password}

    session = requests.Session()
    try:
        response = session.request(
            "POST", login_url, data=payload, headers=flow_constants.HEADERS.value
        )
        return session
    except requests.exceptions.ConnectionError:
        log("⚠️ Erro de coneção. Tentando novamente...")
        login()


@task
def download_files(
    session: requests.Session, sections: str, date: datetime.datetime
) -> list | None:
    """Faz o download dos arquivos .zip com os atos oficiais de cada seção para um dia específico.

    Args:
        session (requests.Session): Instância de Session da biblioteca requests contém os cookies da sessão.
        sections (str): Seções do DOU a serem extraídas (DO1, DO2 e DO3)
        date (datetime.datetime): Data do diário oficial a ser extraído.

    Returns:
        list | None: Lista contendo o caminho dos arquivos .zip baixados ou None.
    """
    download_base_url = flow_constants.DOWNLOAD_BASE_URL.value

    if session.cookies.get("inlabs_session_cookie"):
        cookie = session.cookies.get("inlabs_session_cookie")
    else:
        log("❌ Falha ao obter cookie. Verifique suas credenciais")
        return

    date_to_extract = date.strftime("%Y-%m-%d")

    files = []

    log("⏳️ Realizando download do arquivo ...")
    for dou_section in sections.split(" "):
        final_url = (
            download_base_url
            + date_to_extract
            + "&dl="
            + date_to_extract
            + "-"
            + dou_section
            + ".zip"
        )
        headers = {"Cookie": "inlabs_session_cookie=" + cookie, "origem": "736372697074"}

        response = session.request("GET", final_url, headers=headers)

        if response.status_code == 200:
            file_name = date_to_extract + "-" + dou_section + ".zip"
            file_path = os.path.join(flow_constants.DOWNLOAD_DIR.value, file_name)

            with open(file_path, "wb") as file:
                file.write(response.content)
                files.append(file_path)
                log(f"📁 Arquivo {file_name} salvo.")
        elif response.status_code == 404:
            log(f"❌ Arquivo não encontrado: {date_to_extract + '-' + dou_section + '.zip'}")
            return

    log(f"✅ Download finalizado.")

    return files


@task
def unpack_zip(zip_files: list, output_path: str) -> None:
    """Descompacta os arquivos .zip baixados.

    Args:
        zip_path (str): Caminho para o diretório onde os arquivos .zip estão armazenados.
        output_path (str): Caminho para o diretório onde os arquivos .xml serão armazenados.


    """
    log("⬇️ Iniciando descompactação dos arquivos .zip")
    if zip_files:
        for file in zip_files:
            log(f"Extraindo arquivos de: {file}")
            with zipfile.ZipFile(file, "r") as zip_ref:
                zip_ref.extractall(output_path)
    return


@task
def get_xml_files(xml_dir: str) -> str:
    """Pega as informações dos xml de cada ato oficial.

    Args:
        xml_dir (str): Diretório onde os arquivos .xml estão armazenados.

    Returns:
        str: Caminho do arquivo parquet gerado após o processamento das informações.
    """
    log("📋️ Iniciando processamento dos arquivos .xml...")

    acts = []
    for file in os.listdir(xml_dir):
        if file.endswith(".xml"):
            with open(os.path.join(xml_dir, file), "r", encoding="utf-8") as f:
                xml_data = f.read()
                soup_xml = BeautifulSoup(xml_data, "xml")
                soup_html = BeautifulSoup(soup_xml.find("Texto").text, "html.parser")

                pdf_page = soup_xml.find("article")["pdfPage"]
                edition = soup_xml.find("article")["editionNumber"]
                pubname = soup_xml.find("article")["pubName"]
                number_page = soup_xml.find("article")["numberPage"]
                pub_date = soup_xml.find("article")["pubDate"]
                art_category = soup_xml.find("article")["artCategory"]
                html = soup_html.prettify()
                text = "\n".join(p.get_text() for p in soup_html.find_all("p"))
                assina = soup_html.find_all(class_="assina")
                cargos = soup_html.find_all(class_="cargo")
                identifica = soup_xml.find_all("Identifica")
                text_title = soup_html.find_all(class_="identifica")

                extracted_data = {
                    "title": " ".join([title.text for title in identifica]),
                    "text_title": " ".join(title.get_text() for title in text_title),
                    "published_at": pub_date,
                    "agency": art_category,
                    "text": text,
                    "url": pdf_page,
                    "number_page": number_page,
                    "edition": edition,
                    "section": pubname,
                    "html": html,
                    "signatures": "/".join([signature.text for signature in assina]),
                    "role": " / ".join([cargo.text for cargo in cargos]),
                }
                acts.append(extracted_data)

    df = pd.DataFrame(acts)
    df["extracted_at"] = datetime.datetime.now(pytz.timezone("America/Sao_Paulo")).strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    print(df.sample(5).to_string())
    file_name = f"dou-extraction-{datetime.datetime.now().isoformat(sep='-')}.parquet"
    file_path = os.path.join(flow_constants.OUTPUT_DIR.value, file_name)

    df.to_parquet(file_path, index=False)
    log(f"📁 Arquivo {file_name} salvo.")
    return file_path


@task
def upload_to_datalake(parquet_path: str, dataset: str):
    """Função para realizar a carga dos dados extraídos no datalake da SMS Rio.
    Args:
        parquet_path (str): Caminho para o arquivo parquet com os dados a serem carregados.
        dataset (str): Dataset do BigQuery onde os dados serão carregados.

    Returns:
        bool: True se o upload for bem-sucedido, False caso contrário.
    """

    df = pd.read_parquet(parquet_path)
    log(f"⬆️ Realizando upload de {len(df)} registros para o datalake...")

    if df.empty:
        log("⚠️ Não há registros para processar.")
        return False

    upload_df_to_datalake.run(
        df=df,
        dataset_id=dataset,
        table_id="diarios_uniao_api",
        partition_column="extracted_at",
        if_exists="append",
        if_storage_data_exists="append",
        source_format="parquet",
    )

    log("✅ Upload para o datalake finalizado.")
    return True


@task
def report_extraction_status(status: bool, date: str, environment: str = "dev"):
    """
    Reporta o status da extração para o BigQuery.

    Args:
        status (bool): Indica se a extração foi bem-sucedida (True) ou não (False).
        date (str): Data da extração.
        environment (str, optional): Ambiente de execução (ex: "dev", "prod"). Padrão para "dev".
    """
    log("⬆️ Reportando status da extração...")
    date = parse_date_or_today(date).strftime("%Y-%m-%d")

    success = "true" if status else "false"
    current_datetime = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(
        tzinfo=None
    )
    tipo_diario = "dou-api"

    if environment is None:
        environment = "dev"

    PROJECT = constants.GOOGLE_CLOUD_PROJECT.value[environment]
    DATASET = "projeto_cdi"
    TABLE = "extracao_status"
    FULL_TABLE_NAME = f"`{PROJECT}.{DATASET}.{TABLE}`"

    log(f"Inserting into {FULL_TABLE_NAME} status of success={success} for date='{date}'...")

    client = bigquery.Client()
    query_job = client.query(
        f"""
        INSERT INTO {FULL_TABLE_NAME} (
            data_publicacao, tipo_diario, extracao_sucesso, _updated_at
        )
        VALUES (
            '{date}', '{tipo_diario}', {success}, '{current_datetime}'
        )
    """
    )
    query_job.result()
    log("✅ Status da extração reportado!")


@task
def delete_dirs():
    """
    Deleta os diretórios temporários.
    """
    log("🗑️ Deletando diretórios temporários...")
    shutil.rmtree(flow_constants.DOWNLOAD_DIR.value)
    shutil.rmtree(flow_constants.OUTPUT_DIR.value)
    log("✅ Diretórios temporários deletados.")
