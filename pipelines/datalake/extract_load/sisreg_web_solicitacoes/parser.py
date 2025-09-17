# parser.py
import re
import lxml
from bs4 import BeautifulSoup, SoupStrainer
from pipelines.datalake.extract_load.sisreg_web_solicitacoes.constants import REGEX_TOTAL_PAGINAS, REGEX_TOTAL_REGISTROS


def parse_codigos_sisreg(html: str):
    """
    Return: (rows, total_pages, total_registros)
      - rows: [{"cod_solicitacao": "<code>"}]
      - total_pages: int | None
      - total_registros: int | None
    """

    # obtem os totais (busca direto no html cru inteiro)
    m = REGEX_TOTAL_REGISTROS.search(html)
    total_registros = int(m.group(1)) if m else None

    m = REGEX_TOTAL_PAGINAS.search(html)
    total_pages = int(m.group(1)) if m else None

    # cria o parser apenas para as tags <tr> com onclick
    only_tr = SoupStrainer("tr")
    soup = BeautifulSoup(html, "lxml", parse_only=only_tr)

    # obtem as linhas de dados
    rows = []
    for tr in soup.find_all("tr"):
        # verifica se a linha tem o atributo onclick com "visualizaFicha("
        oc = tr.get("onclick", "")

        if "visualizaFicha(" not in oc:
            # se a linha não tiver onclick com "visualizaFicha(", ignora
            continue

        td = tr.find("td")
        if not td:
            # se a linha não tiver <td>, ignora
            continue

        # extrai o texto do código e normaliza espaços em branco
        cod = re.sub(r"\s+", " ", td.get_text(strip=True))

        # adiciona o código na lista
        rows.append({"cod_solicitacao": cod})

    return rows, total_pages, total_registros
