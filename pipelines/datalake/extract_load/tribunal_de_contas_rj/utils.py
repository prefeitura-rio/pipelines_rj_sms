# -*- coding: utf-8 -*-
import re
from typing import List, Optional, Tuple, Union

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
from urllib3.util.retry import Retry

from pipelines.utils.logger import log

from .dns import HostHeaderSSLAdapter


def split_case_number(case_num: str) -> tuple:
    # Exemplo: '040/100420/2019'
    case_regex = re.compile(r"(?P<sec>[0-9]+)/(?P<num>[0-9]+)/(?P<year>[0-9]{4})")
    m = case_regex.search(case_num.strip())
    # Padding para transformar "40" -> "040"
    sec = m.group("sec").rjust(3, "0")
    num = m.group("num")
    year = m.group("year")
    assert len(year) == 4, f"Year '{year}' has length {len(year)}; expected 4"
    return (sec, num, year)


def send_request(
    method: str, url: str, data: Optional[dict] = None, expected_type: str = "html"
) -> Tuple[Union[str, Optional[BeautifulSoup]]]:
    method = method.strip().upper()
    log(f"Sending {method} request expecting '{expected_type}' response: {url}")

    # Alguns servidores de DNS parecem não ter o IP do site do TCM
    # Quando isso acontece, ficamos presos em ConnectionError
    # Então, usamos um Adapter que tenta vários (Cloudflare, Google, etc)
    # e usa o primeiro IP que encontrar
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=15, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HostHeaderSSLAdapter(max_retries=retries))

    res = None
    if method == "POST":
        res = session.post(url=url, data=data)
    elif method == "GET":
        res = session.get(url=url)
    res.raise_for_status()
    # [Ref] https://stackoverflow.com/a/52615216/4824627
    res.encoding = res.apparent_encoding

    ALLOWED_CONTENT_TYPES = ["application/json", "text/html"]

    # Precisamos quebrar em ';' porque às vezes termina em '; charset=UTF-8'
    ct = res.headers["Content-Type"].split(";")[0].lower()
    if ct not in ALLOWED_CONTENT_TYPES:
        log(f"Got disallowed Content-Type '{ct}' for URL '{url}'", level="warning")
        return (res.url, None)

    if expected_type == "json":
        if ct != "application/json":
            log(f"Expected Content-Type 'application/json'; got '{ct}'", level="warning")
        log(f"Got response of size {len(res.text)}")
        return (res.url, res.json())

    if expected_type == "html":
        if ct != "text/html":
            log(f"Expected Content-Type 'text/html'; got '{ct}'", level="warning")
        log(f"Got response of size {len(res.text)}")
        return (res.url, BeautifulSoup(res.text, "html.parser"))

    return (res.url, res.text)


def find_h5_from_text(root: BeautifulSoup, match: str):
    all_h5s = root.find_all("h5")
    for h5 in all_h5s:
        m = re.match(match, h5.get_text().strip(), re.IGNORECASE)
        if m:
            return h5
    return None


def get_table_rows_from_h5(h5: BeautifulSoup):
    # Odeio isso, mas acho que é a única forma de fazer com tantos elementos genéricos
    table_wrapper = h5.parent.parent.find_next_sibling("div", attrs={"class", "row"})
    table = table_wrapper.find("table")
    # Se não encontramos, retorna
    if table is None:
        return None
    # Retorna todos os <tr> em <tbody>
    return table.find("tbody").find_all("tr")


def cleanup_text(text: str) -> str:
    text = str(text)

    EMPTY = ""
    SPACE = " "
    replace_pairs = [
        ("\u00ad", EMPTY),  # Soft Hyphen
        ("\u200c", EMPTY),  # Zero Width Non-Joiner
        ("\t", SPACE),  # Tab
        ("\n", SPACE),  # Line feed
        ("\u00a0", SPACE),  # No-Break Space
    ]
    for pair in replace_pairs:
        text = text.replace(pair[0], pair[1])

    # NULL, \r, etc
    text = re.sub(r"[\u0000-\u001F]", EMPTY, text)
    # DEL, outros de controle
    text = re.sub(r"[\u007F-\u009F]", EMPTY, text)
    # Espaços de tamanhos diferentes
    text = re.sub(r"[\u2000-\u200B\u202F\u205F]", SPACE, text)
    # LTR/RTL marks, overrides
    text = re.sub(r"[\u200E-\u202E]", EMPTY, text)
    # Espaços repetidos
    text = re.sub(r"\s+", SPACE, text)

    return text.strip()


def get_counselors_initials() -> List[str]:
    # Página de conselheiros
    _, html = send_request(
        "GET", "https://www.tcmrio.tc.br/web/site/Destaques.aspx?group=Conselheiros"
    )
    # Remove conselheiros aposentados
    html.find("div", {"id": "galeria_conselheiros"}).decompose()
    # Nomes completos de conselheiros
    counselors_full = [
        span.get_text().strip() for span in html.find_all("span", {"class": "conselheiros"})
    ]
    # Iniciais de conselheiros
    counselors = [
        # Pega somente letras maiúsculas
        re.sub(
            r"[^A-Z]",
            "",
            # Remove acentos (ex.: 'Á' -> 'A')
            unidecode(counselor, replace_str=""),
        )
        for counselor in counselors_full
    ]
    logstr = ", ".join(
        [f"{full_name} ({initials})" for full_name, initials in zip(counselors_full, counselors)]
    )
    log(f"Found {len(counselors_full)} counselors: {logstr}")
    return counselors
