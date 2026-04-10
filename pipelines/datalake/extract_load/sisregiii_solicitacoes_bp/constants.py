# -*- coding: utf-8 -*-

BASE_URL = "https://sisregiii.saude.gov.br"
GERENCIADOR_URL = BASE_URL + "/cgi-bin/gerenciador_solicitacao"
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'


INFISICAL_PATH = "/sisreg"
INFISICAL_VITACARE_USERNAME = "SISREG_USER"
INFISICAL_VITACARE_PASSWORD = "SISREG_PASSWORD"

TIMEOUT_LOGIN = 30
TIMEOUT_CONSULTA = 180
TEMPO_ESPERA_SISREG = 8.5
LIMITE_REQUISICOES_SESSAO = 15
MAX_TENTATIVAS_REEXTRA = 100

HEADERS_DISFARCE = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}

STATUS_SISREG = {
    "PENDENTE": "1", "CANCELADO": "3", "DEVOLVIDO": "4",
    "REENVIADO": "5", "NEGADO": "6", "APROVADO": "7", "CANCELADO2": "10"
}

CONFIGS_EXTRACAO_BASE = [
    {"tipo_periodo": "A", "codigo_situacao": "7", "status_coluna": "AUTORIZADO"},
    {"tipo_periodo": "E", "codigo_situacao": "7", "status_coluna": "EXECUTADO"},
]

for nome, cod in STATUS_SISREG.items():
    CONFIGS_EXTRACAO_BASE.append({
        "tipo_periodo": "S", "codigo_situacao": cod,
        "status_coluna": "CANCELADO" if nome == "CANCELADO2" else nome
    })

TRADUTOR_COLUNAS_SISREG = {
    'cod_solicitac': 'cod_solicitacao', 
    'cod_solicitacao': 'cod_solicitacao',
    'data': 'data_da_solicitacao', 
    'data_solicitacao': 'data_da_solicitacao',
    'municipio_residencia': 'municipio', 
    'municipio': 'municipio',
    'situacao_atual': 'situacao', 
    'sit': 'situacao',
    'situacao': 'situacao',
    'data_execucao': 'data_da_execucao', 
    'dt_exec': 'data_da_execucao',
    'data_autorizacao': 'data_da_autorizacao', 
    'dt_aut': 'data_da_autorizacao'
}

DATASET_ID_BRUTO = 'brutos_sisreg_solicitacoes'
TABLE_ID_BP = 'tb_sisreg_solicitacoes_bp'