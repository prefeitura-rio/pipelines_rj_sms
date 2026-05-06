# -*- coding: utf-8 -*-
"""
Constants
"""
ELASTIC_INDEX = "solicitacao-ambulatorial-rj"
ELASTIC_HOST = "sisreg-es.saude.gov.br"
ELASTIC_PORT = 443
ELASTIC_SCHEME = "https"

BASE_URL = "https://sisregiii.saude.gov.br/cgi-bin"
LOGIN_URL = f"{BASE_URL}/index"
AUTORIZADOR_URL = f"{BASE_URL}/autorizador"

DEFAULT_DATASET_ID = "brutos_sisreg_fila_e_vagas"
DEFAULT_FILA_E_VAGAS_TABLE_ID = "tb_sisreg_fila_e_vagas"
DEFAULT_VAGAS_DETALHADAS_TABLE_ID = "tb_sisreg_vagas_detalhadas"
DEFAULT_REQUEST_DELAY = 7

DEFAULT_MAX_ES_PAGES = None
DEFAULT_MAX_PROCEDIMENTOS = None

EXTRACTION_DATE_COLUMN = "dt_extracao"

INFISICAL_SISREG_API_PATH = "/sisreg_api"
INFISICAL_SISREG_API_USERNAME = "ES_USERNAME"
INFISICAL_SISREG_API_PASSWORD = "ES_PASSWORD"

INFISICAL_SISREG_PATH = "/sisreg_regulacao"
INFISICAL_SISREG_USERNAME = "SISREG_USER"
INFISICAL_SISREG_PASSWORD = "SISREG_PASSWORD"


SESSION_HEADERS = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 OPR/118.0.0.0"
        }

DEFAULT_QUERY = {
    "query": {
        "bool": {
            "must": [
                {"match": {"codigo_central_reguladora": "330455"}},
                {"terms": {"sigla_situacao": ["P", "R", "D"]}},
            ]
        }
    },
    "size": 10000,
    "sort": [{"_id": "asc"}],
}
