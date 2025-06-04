# -*- coding: utf-8 -*-
import re
from datetime import datetime
from typing import List, Optional

import pytz
import requests
from bs4 import BeautifulSoup, NavigableString
from dateutil import parser

from pipelines.utils.logger import log


def standardize_date_from_string(date: str) -> str:
    # Tenta fazer parsing da string recebida, pode dar erro
    dateobj = parser.parse(date)
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

    # Parece que alguns anexos são diretamente PDFs; não sei se alguma outra
    # coisa esquisita pode vir. Então filtramos por Content Type
    ALLOWED_CONTENT_TYPES = [
        "application/json",
        "text/html"
    ]
    # Precisamos quebrar em ';' porque às vezes termina em '; charset=UTF-8'
    ct = res.headers["Content-Type"].split(";")[0].lower()
    if ct not in ALLOWED_CONTENT_TYPES:
        log(f"Got disallowed Content-Type '{ct}' for URL '{url}'", level="warning")
        return None

    if type == "json":
        if ct != "application/json":
            log(f"Expected Content-Type 'application/json'; got '{ct}'", level="warning")
        return res.json()

    if type == "html":
        if ct != "text/html":
            log(f"Expected Content-Type 'text/html'; got '{ct}'", level="warning")
        return BeautifulSoup(res.text, "html.parser")

    return res.text


def get_all_links_in_folder(folder: BeautifulSoup) -> List[BeautifulSoup]:
    REGEX_DIGITS = re.compile(r"[0-9]+")
    # A estrutura é:
    # <li ...>
    #   <span class="folder"> ... </span>
    #   <ul ...>
    #     <!-- Cada li pode ser uma sub-pasta; eventualmente há um 'file' -->
    #     <li>
    #       <span class="file">
    #         <a class="linkMateria" ...> ... </a>
    #       </span>
    #     </li>
    #   </ul>
    # </li>
    root = folder.parent
    return list(
        set(
            root.find_all("a", attrs={"identificador": REGEX_DIGITS})
            + root.find_all("a", attrs={"data-materia-id": REGEX_DIGITS})
        )
    )


def get_links_for_path(folders: List[BeautifulSoup], path: list) -> List[BeautifulSoup]:
    # Se chegamos no final da recursão
    if len(path) == 0:
        # Para cada pasta que recebemos
        links = []
        for folder in folders:
            # Pega todos os links nela, e guarda junto com os outros
            links.extend(get_all_links_in_folder(folder))
        return links

    # Se ainda temos mais recursão pela frente, pega a primeira pasta do caminho
    first_folder_name = path[0]
    # Para cada pasta que recebemos
    for folder in folders:
        # Pega o 'nome' da pasta
        text = folder.get_text().lower().strip()
        # Confere se bate com o que queremos
        if text == first_folder_name:
            # Se sim, pega somente as pastas dentro dessa
            subfolders = folder.parent.find_all("span", attrs={"class": "folder"})
            # ...e remove a própria do caminho
            return get_links_for_path(subfolders, path[1:])
    # Se chegamos aqui, não encontramos a pasta especificada pelo caminho
    return []


def get_links_if_match(folders: List[BeautifulSoup], regex_search_str: str) -> List[BeautifulSoup]:
    all_links = []
    for folder in folders:
        # Pega o 'nome' da pasta
        text = folder.get_text().strip()
        # Confere se bate com o que queremos
        if re.search(regex_search_str, text, re.IGNORECASE | re.MULTILINE):
            # Pega todos os <a> com atributos que estamos procurando
            # que estão 'dentro' da pasta
            all_links.extend(get_all_links_in_folder(folder))

    return all_links


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
