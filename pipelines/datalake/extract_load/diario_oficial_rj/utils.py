# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Optional

import pytz
import requests
from bs4 import BeautifulSoup, NavigableString
from dateutil import parser

from pipelines.utils.logger import log


def standardize_date_from_string(date: str) -> str:
    # Tenta fazer parsing da string recebida, pode dar erro
    dateobj = parser.parse(date, dayfirst=True)
    # Pega data no formato YYYY-MM-DD, formato que a API recebe
    return dateobj.strftime("%Y-%m-%d")


def get_today() -> str:
    # Retorna a data atual em YYYY-MM-DD, formato que a API recebe
    current_time = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
    return current_time.strftime("%Y-%m-%d")


def send_get_request(url: str, type: Optional[str]):
    if type:
        log(f"Sending GET request expecting '{type}' response: {url}")
    else:
        log(f"Sending GET request: {url}")

    res = requests.get(url=url)
    res.raise_for_status()
    # [Ref] https://stackoverflow.com/a/52615216/4824627
    res.encoding = res.apparent_encoding

    if type == "json":
        return res.json()

    if type == "html":
        return BeautifulSoup(res.text, "html.parser")

    return res.text


def string_cleanup(string: str) -> str:
    return str(string).replace("\xa0", " ").replace("\r", "")


def node_cleanup(root: BeautifulSoup) -> BeautifulSoup:
    # Para pegar o conteúdo textual, remove elementos inline
    for inline_tag in ["a", "b", "i", "em", "span"]:
        for tag in root.find_all(inline_tag):
            parent = tag.parent
            tag.unwrap()  # Remove a tag
            # Infelizmente isso cria vários nós de texto soltos
            # Precisamos juntá-los manualmente
            merged_strings = []
            # Para cada nó filho
            for child in parent.children:
                # Se for uma string solta, salva
                if isinstance(child, NavigableString):
                    merged_strings.append(child)
                # Senão, confere se já temos 2 ou mais strings
                elif len(merged_strings) >= 2:
                    # Se sim, junta as strings e cria uma nova
                    merged_text = "".join(merged_strings)
                    new_string = NavigableString(merged_text)
                    merged_strings[0].replace_with(new_string)
                    # Remove as antigas
                    for s in merged_strings[1:]:
                        s.extract()
                    merged_strings = []
            # Se ainda tínhamos strings soltas, junta todas
            if len(merged_strings) > 1:
                merged_text = "".join(merged_strings)
                new_string = NavigableString(merged_text)
                merged_strings[0].replace_with(new_string)
                for s in merged_strings[1:]:
                    s.extract()

    return root
