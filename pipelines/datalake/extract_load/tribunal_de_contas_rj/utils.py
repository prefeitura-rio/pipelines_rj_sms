# -*- coding: utf-8 -*-
import re
import time
from typing import Optional

import requests
from bs4 import BeautifulSoup

from pipelines.datalake.extract_load.tribunal_de_contas_rj.dns import (
    HostHeaderSSLAdapter,
)
from pipelines.utils.logger import log


def split_case_number(case_num: str) -> tuple:
    # Exemplo: '040/100420/2019'
    case_regex = re.compile(r"(?P<sec>[0-9]+)/(?P<num>[0-9]+)/(?P<year>[0-9]{4})")
    m = case_regex.search(case_num.strip())
    sec = m.group("sec")
    num = m.group("num")
    year = m.group("year")
    return (sec, num, year)


def send_request(method: str, url: str, data: Optional[dict] = None):
    method = method.strip().upper()
    log(f"Sending {method} request expecting 'text/html' response: {url}")

    # Alguns servidores de DNS parecem não ter o IP do site do TCM
    # Quando isso acontece, ficamos presos em ConnectionError
    # Então, usamos um Adapter que tenta vários (Cloudflare, Google, etc)
    # e usa o primeiro IP que encontrar
    session = requests.Session()
    session.mount("https://", HostHeaderSSLAdapter())

    res = None
    if method == "POST":
        res = session.post(url=url, data=data)
    elif method == "GET":
        res = session.get(url=url)
    res.raise_for_status()

    # [Ref] https://stackoverflow.com/a/52615216/4824627
    res.encoding = res.apparent_encoding

    # Precisamos quebrar em ';' porque às vezes termina em '; charset=UTF-8'
    ct = res.headers["Content-Type"].split(";")[0].lower()
    if ct != "text/html":
        log(f"Expected Content-Type 'text/html'; got '{ct}' for URL '{url}'", level="warning")
        return None

    log(f"Got HTML response of size {len(res.text)}")
    return BeautifulSoup(res.text, "html.parser")


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
