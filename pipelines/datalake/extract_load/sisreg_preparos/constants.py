# -*- coding: utf-8 -*-
"""
Constants
"""
EXTRACTION_DATE_COLUMN = "data_extracao"

DEFAULT_DATASET_ID = "brutos_sisreg_preparos"
DEFAULT_PREPAROS_TABLE_ID = "tb_preparos"
DEFAULT_HISTORICO_TABLE_ID = "tb_historicos_preparos"

# Quantidade padrão de unidades a processar (None = todas)
DEFAULT_UNIDADES_LIMIT = None


INFISICAL_SISREG_PATH = "/sisreg"
INFISICAL_SISREG_USERNAME = "SISREG_USER"
INFISICAL_SISREG_PASSWORD = "SISREG_PASSWORD"

ETAPA_PREPARO        = 'FRM_PREPARO'

SISREG_BASE_URL = "https://sisregiii.saude.gov.br"
SISREG_LOGIN_URL = f"{SISREG_BASE_URL}/cgi-bin/index"
SISREG_PREPAROS_URL = "http://sisregiii.saude.gov.br/cgi-bin/config_preparo"


REQUEST_HEADERS = {
   'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0',
   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
   'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
   'Accept-Encoding': 'gzip, deflate, br, zstd',
   'Sec-GPC': '1',
   'Connection': 'keep-alive',
   'Upgrade-Insecure-Requests': '1',
   'Sec-Fetch-Dest': 'document',
   'Sec-Fetch-Mode': 'navigate',
   'Sec-Fetch-Site': 'none',
   'Sec-Fetch-User': '?1',
   'Priority': 'u=0 i'
}
import time

