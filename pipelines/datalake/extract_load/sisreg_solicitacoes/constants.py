# -*- coding: utf-8 -*-

BASE_URL = "https://sisregiii.saude.gov.br"
GERENCIADOR_URL = BASE_URL + "/cgi-bin/gerenciador_solicitacao"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    " Chrome/120.0.0.0 Safari/537.36"
)

INFISICAL_PATH = "/sisreg"
INFISICAL_USERNAME = "SISREG_USER"
INFISICAL_PASSWORD = "SISREG_PASSWORD"

HEADERS_DISFARCE = {
    "User-Agent": USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

STATUS_SISREG_MAPPING = {
    "PENDENTE": "1",
    "CANCELADO": "3",
    "DEVOLVIDO": "4",
    "REENVIADO": "5",
    "NEGADO": "6",
    "APROVADO": "7",
    "CANCELADO2": "10",
}


RUN_CONFIGS = {
    "solicitacoes_autorizadas": {
        "tipo_pedido": "A",
        "codigo_situacao": "7",
        "status_coluna": "AUTORIZADO",
    },
    "solicitacoes_executadas": {
        "tipo_pedido": "E",
        "codigo_situacao": "7",
        "status_coluna": "EXECUTADO",
    },
}


for nome, cod in STATUS_SISREG_MAPPING.items():
    chave = f"solicitacoes_{nome.lower()}"
    RUN_CONFIGS[chave] = {
        "tipo_pedido": "S",
        "codigo_situacao": cod,
        "status_coluna": "CANCELADO" if nome == "CANCELADO2" else nome,
    }

DATASET_ID_BRUTO = "brutos_sisreg_solicitacoes"
TABLE_ID_BP = "tb_sisreg_solicitacoes_bp"