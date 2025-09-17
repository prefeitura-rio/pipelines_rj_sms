from pathlib import Path
import re

# Páginas e URLs
BASE_URL = "https://sisregiii.saude.gov.br"
LOGIN_PAGE = f"{BASE_URL}/cgi-bin/index"  # GET
LOGIN_POST = f"{BASE_URL}/"  # POST
GERENCIADOR_URL = f"{BASE_URL}/cgi-bin/gerenciador_solicitacao"
CONFIG_PERFIL_URL = f"{BASE_URL}/cgi-bin/config_perfil"

# Parâmetros da consulta
DT_INICIAL = "02/06/2025"
DT_FINAL = "02/06/2025"
TIPO_PERIODO = "S"  # 'S' = SOLICITAÇÃO
SITUACAO = "1"  # '1' = SOL/PEN/REG
PAGE_SIZE = 0  # 10,20,50,100 ou 0=TODOS
ORDENACAO = "2"
MAX_PAGES = None  # None = até o fim
WAIT_SECONDS = 0.5  # tempo entre requisições de páginas
OUT_PATH = Path("solicitacoes.csv")

# Regexes
# encontra a string "Página X de N"
REGEX_TOTAL_PAGINAS = re.compile(
    r"Página\s+\d+\s+de\s+(\d+)\b", re.I
)

# encontra a string "SOLICITAÇÕES RETORNADAS (N)"
REGEX_TOTAL_REGISTROS = re.compile(
    r"SOLICITA(?:C|Ç|ÇÕ)ES\s+RETORNADAS\s*\((\d+)\)", re.I
)
