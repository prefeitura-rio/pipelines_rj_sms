# -*- coding: utf-8 -*-
import re
from typing import Optional

import requests
from bs4 import BeautifulSoup

from pipelines.utils.logger import log


def split_process_number(process_num: str) -> tuple:
    # Exemplo: '040/100420/2019'
    process_regex = re.compile(r"(?P<sec>[0-9]+)/(?P<num>[0-9]+)/(?P<year>[0-9]{4})")
    m = process_regex.search(process_num.strip())
    sec = m.group("sec")
    num = m.group("num")
    year = m.group("year")
    return (sec, num, year)


def send_post_request(url: str, data: Optional[dict]):
    log(f"Sending POST request expecting 'text/html' response: {url}")

    res = requests.post(url=url, data=data)
    res.raise_for_status()
    # [Ref] https://stackoverflow.com/a/52615216/4824627
    res.encoding = res.apparent_encoding

    # Precisamos quebrar em ';' porque às vezes termina em '; charset=UTF-8'
    ct = res.headers["Content-Type"].split(";")[0].lower()
    if ct != "text/html":
        log(f"Expected Content-Type 'text/html'; got '{ct}' for URL '{url}'", level="warning")
        return None

    return BeautifulSoup(res.text, "html.parser")
