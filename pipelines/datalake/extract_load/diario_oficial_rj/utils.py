# -*- coding: utf-8 -*-
import datetime
import re
from typing import List, Optional

import pytz
import requests
from bs4 import BeautifulSoup, NavigableString
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.constants import constants
from pipelines.utils.logger import log
from pipelines.utils.time import parse_date_or_today


def report_extraction_status(status: bool, date: str, environment: str = "dev"):
    date = parse_date_or_today(date).strftime("%Y-%m-%d")
    success = "true" if status else "false"
    current_datetime = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo")).replace(
        tzinfo=None
    )

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
            '{date}', 'dorj', {success}, '{current_datetime}'
        )
    """
    )
    query_job.result()  # Wait for the job to complete
    log("Extraction report done!")


def send_get_request(url: str, type: Optional[str]):
    if type:
        log(f"Sending GET request expecting '{type}' response: {url}")
    else:
        log(f"Sending GET request: {url}")

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=15, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    res = None
    try:
        res = session.get(url=url)
        res.raise_for_status()
    except Exception as exc:
        if res is not None:
            log(f"Request returned status HTTP {res.status_code}", level="error")
        else:
            log(f"Error requesting URL: {repr(exc)}", level="error")
        return Exception()
    # [Ref] https://stackoverflow.com/a/52615216/4824627
    res.encoding = res.apparent_encoding

    # Parece que alguns anexos são diretamente PDFs; não sei se alguma outra
    # coisa esquisita pode vir. Então filtramos por Content Type
    ALLOWED_CONTENT_TYPES = ["application/json", "text/html"]
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
            if folder is None:
                continue
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
            # subfolders = folder.parent.find_all("span", attrs={"class": "folder"})
            # Se sim, pega somente descendentes diretos da pasta
            # <span class="folder">...</span>  <-- Estamos aqui
            # <ul>
            #     <li>
            #         <div ...></div>
            #         <span class="folder">...</span>  <-- Queremos chegar aqui
            subfolders = [
                li.find("span", attrs={"class": "folder"})
                for li in folder.find_next_sibling("ul").find_all("li", recursive=False)
            ]
            # ...e remove a própria do caminho
            return get_links_for_path([folder, *(subfolders or [])], path[1:])
    # Se chegamos aqui, não encontramos a pasta especificada pelo caminho
    return []


def node_cleanup(root: BeautifulSoup) -> BeautifulSoup:
    # Para pegar o conteúdo textual, remove elementos inline
    for tag in root.find_all(["a", "b", "i", "em", "span"]):
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
                    s.decompose()
                merged_strings = []
            # Se só tínhamos uma string, ela fica como é
            else:
                merged_strings = []
        # Se ainda tínhamos strings soltas, junta todas
        if len(merged_strings) > 1:
            merged_text = "".join(merged_strings)
            new_string = NavigableString(merged_text)
            merged_strings[0].replace_with(new_string)
            for s in merged_strings[1:]:
                s.decompose()

    return root


def filter_paragraphs(arr: List[str], type: str) -> List[str]:
    # from itertools import groupby
    # groups = []
    # for _, group in groupby(arr, lambda x: len(x) == 0):
    #     groups.append(list(group))
    # groups

    # Limpa qualquer string vazia
    arr = [text for text in arr if len(text)]

    def filter_for_regex(arr: List[str], regex: List[str]) -> List[str]:
        new_arr = []
        for text in arr:
            ignore = False
            for r in regex:
                if re.search(r, text, re.IGNORECASE):
                    ignore = True
                    break
            if not ignore:
                new_arr.append(text)
        return new_arr

    remove_header = [
        r"^atos? d[ao]",
        r"^subsecretaria",
        r"^superintendência",
        r"^administração setorial",
        r"^tribunal de contas do município do rio de janeiro$",
        r"^controladoria geral do município$",
        r"^subcontroladoria de",
        r"^coordenadoria técnica",
        r"^secretári[ao]",
        r"^controladora?",
        r"^rio de janeiro, [0-9]+ de",
    ]
    remove_body = [
        r"^decreta:?$",
        r"^resolve:?$",
        r"^rio de janeiro, [0-9]+ de",
    ]
    if type == "header":
        return filter_for_regex(arr, remove_header)
    return filter_for_regex(arr, remove_body)


def parse_do_contents(root: BeautifulSoup) -> List[str]:
    # TODO: lidar com tabelas
    for table in root.find_all("table"):
        # Pega último ancestral; só o [document] tem .new_tag()
        document = None
        for parent in root.parents:
            document = parent
        p = document.new_tag("p")
        p.string = "[tabela]"
        table.replace_with(p)
        table.decompose()

    def clean_text(text: str):
        text = str(text).strip()
        replace_list = [
            ("\u00A0", " "),  # NO-BREAK SPACE
            ("\u202F", " "),  # NARROW NO-BREAK SPACE
            ("\r", ""),
            ("\n", " "),
        ]
        for fro, to in replace_list:
            text = text.replace(fro, to)
        return text

    def is_text_irrelevant(text: str):
        # Strings somente de pontos/espaços
        # Normalmente aparece em casos de alteração de legislação
        # Ex.: 1200504
        if re.match(r"^[\.\s]+$", text) is not None:
            return True
        return False

    all_ps = root.find_all("p")

    section_index = -1
    block_index = -1
    p_index = -1
    is_in_header = False
    new_block = False
    sections = dict()
    for p in all_ps:
        text = clean_text(p.get_text())
        if len(text) <= 0:
            new_block = True
            continue
        # Confere linhas que não indicam novo bloco,
        # mas que não precisamos adicionar ao banco de dados
        if is_text_irrelevant(text):
            continue
        # Substituto de `i` em `for (i, p) in enumerate(...)`
        # mas ignorando parágrafos vazios
        p_index += 1

        # Se encontramos um parágrafo centralizado
        is_centered = (
            p.attrs.get("align") == "center"
            or re.search(r"text\-align\s*\:\s*center", p.attrs.get("style") or "") is not None
        )
        # Trata o primeiro parágrafo do texto sempre como um cabeçalho
        if is_centered or p_index == 0:
            # Então estamos no início ou fim de uma seção
            # Se já não estávamos numa seção
            if not is_in_header:
                # Flag que estamos, incrementa índice
                is_in_header = True
                section_index += 1
                new_block = True
                block_index = -1
                sections[section_index] = {"header": [], "body": []}
            sections[section_index]["header"].append(text)
            continue

        is_in_header = False

        if section_index == -1:
            # Temos corpo sem cabeçalho antecedendo
            section_index = 0
        if section_index not in sections:
            sections[section_index] = {"header": [], "body": []}

        if new_block:
            block_index += 1
            new_block = False
        if block_index == -1:
            block_index = 0
        # Garante que body[block_index] é um array
        while len(sections[section_index]["body"]) < (block_index + 1):
            sections[section_index]["body"].append([])

        sections[section_index]["body"][block_index].append(text)

    # `sections` é algo como:
    # {
    #   '0': {
    #     'header': ['...', ...],
    #     'body': [
    #       ['...', ...],
    #       ['...', ...],
    #       ...
    #     ]
    #   }
    #   ...
    # }
    for section_k in sections.keys():
        # k = header, body / v = array de parágrafos
        for area_k, area_v in sections[section_k].items():
            if len(area_v) <= 0:
                continue
            if isinstance(area_v[0], str):
                # Filtra parágrafos pelo que temos interesse
                sections[section_k][area_k] = filter_paragraphs(area_v, area_k)
                continue
            # Estamos no corpo e temos vários blocos, cada um com múltiplos parágrafos
            # Para cada bloco
            for i, block in enumerate(area_v):
                # Filtra parágrafos
                sections[section_k][area_k][i] = filter_paragraphs(block, area_k)
        # Se o corpo veio vazio
        if len(sections[section_k]["body"]) <= 0:
            # Apaga essa entrada
            sections[section_k] = None
        else:
            # Cabeçalho deve ser uma string
            sections[section_k]["header"] = "\n".join(sections[section_k]["header"])
            for i, arr in enumerate(sections[section_k]["body"]):
                sections[section_k]["body"][i] = [sub_arr for sub_arr in arr if len(sub_arr)]

    return_ = [obj for _, obj in sections.items() if obj is not None]
    return return_
