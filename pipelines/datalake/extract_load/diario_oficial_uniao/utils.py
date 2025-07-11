# -*- coding: utf-8 -*-
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.utils.logger import log


def extract_decree_details(text_link_href: str) -> dict:
    """
    Função para extrair detalhes de um decreto a partir do link do texto.
    #### Parameters:
    text_link_href: URL do ato oficial para extração do texto e outras informações.

    #### Returns
    dict: Um dicionário contendo os detalhes do ato oficial.

    """
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504, 104])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    decree_text = decree_date = decree_edition = decree_section = decree_page = decree_agency = None

    try:
        response = session.get(text_link_href)
        soup = BeautifulSoup(response.text, "html.parser")

        decree_text = soup.find(class_="texto-dou")
        decree_date = soup.find(class_="publicado-dou-data")
        decree_edition = soup.find(class_="edicao-dou-data")
        decree_section = soup.find(class_="secao-dou")
        decree_page = soup.find(class_="secao-dou-data")
        decree_agency = soup.find(class_="orgao-dou-data")

        return {
            "published_at": decree_date.text if decree_date else "",
            "edition": decree_edition.text if decree_edition else "",
            "section": decree_section.text if decree_section else "",
            "agency": decree_agency.text if decree_agency else "",
            "page": decree_page.text if decree_page else "",
            "text": decree_text.text if decree_text else "",
            "html": str(decree_text) if decree_text else "",
            "url": text_link_href,
            "_extracted_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
    except Exception as e:
        log(f"Erro durante a extração de {text_link_href}: {str(e)}")
        raise e
        return {
            "published_at": f"Erro: {str(e)}",
            "edition": f"Erro: {str(e)}",
            "section": f"Erro: {str(e)}",
            "agency": f"Erro: {str(e)}",
            "page": f"Erro: {str(e)}",
            "text": f"Erro: {str(e)}",
            "html": f"Erro: {str(e)}",
            "url": text_link_href,
        }
    finally:
        session.close()
