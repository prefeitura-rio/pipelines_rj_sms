# -*- coding: utf-8 -*-
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def extract_decree_details(text_link_href: str) -> dict:
    """
    Função para extrair detalhes de um decreto a partir do link do texto.
    #### Parameters:
    text_link_href: URL do ato oficial para extração do texto e outras informações.

    #### Returns
    dict: Um dicionário contendo os detalhes do ato oficial.

    """
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=15, status_forcelist=[500, 502, 503, 504, 104])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    decree_text = decree_date = decree_edition = decree_section = decree_page = decree_agency = None

    try:
        response = session.get(text_link_href)
        soup = BeautifulSoup(response.text, "html.parser")

        decree_title = soup.find(class_="portlet-title-text border-bottom-0")
        decree_text = soup.find(class_="texto-dou")
        decree_identifiers = soup.find_all(
            class_="identifica"
        )  # Título dentro do texto. Em alguns casos não é igual ao title
        decree_date = soup.find(class_="publicado-dou-data")
        decree_edition = soup.find(class_="edicao-dou-data")
        decree_section = soup.find(class_="secao-dou")
        decree_page = soup.find(class_="secao-dou-data")
        decree_agency = soup.find(class_="orgao-dou-data")
        decree_signatures = soup.find_all(
            class_="assina"
        )  # Quem assina o decreto (Caso haja assinatura)

        return {
            "title": decree_title.text if decree_title else "",
            "text_title": " ".join([identifier.text for identifier in decree_identifiers]),
            "signatures": ";".join([signature.text for signature in decree_signatures]),
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
        raise e
    finally:
        session.close()
