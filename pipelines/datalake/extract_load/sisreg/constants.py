# -*- coding: utf-8 -*-
"""
Constantes e perfis de comportamento do pacote SISREG unificado.

Centraliza todos os valores fixos: dataset/tabela destino, URLs, cabecalhos HTTP,
mapa de perfil de credencial -> caminho Infisical, perfis de politeness (delay,
timeout, orcamento de requisicoes) e janela de extracao. Nenhuma logica aqui.
"""

# ---------------------------------------------------------------------------
# Destino no datalake
# ---------------------------------------------------------------------------

# Dataset unico onde todos os extratores escrevem.
DATASET_ID_PADRAO = "brutos_sisreg_web"

# Nomes padrao de tabela por conjunto (parametrizaveis no fluxo).
TABELA_ESCALAS = "escalas"
TABELA_AFASTAMENTOS = "afastamentos"
TABELA_AFASTAMENTO_HISTORICO = "afastamento_historico"
TABELA_PREPAROS = "preparos"
TABELA_SOLICITACOES = "solicitacoes"
TABELA_FILA_E_VAGAS = "fila_e_vagas"
TABELA_VAGAS_DETALHADAS = "vagas_detalhadas"

# Tabela de log de execucoes (auditoria de frescor/completude).
TABELA_LOG_EXECUCAO = "log_execucao_sisreg"

# Coluna de particao usada em todos os conjuntos (excecoes documentadas no registry).
COLUNA_PARTICAO = "data_extracao"

# ---------------------------------------------------------------------------
# Janela de extracao
# ---------------------------------------------------------------------------

# Quantos dias para tras cada execucao re-extrai (nao e backfill completo).
# Valor alinhado com sisreg_api. Ajustavel por parametro de fluxo.
JANELA_DIAS = 180

# ---------------------------------------------------------------------------
# URLs base
# ---------------------------------------------------------------------------

URL_BASE = "https://sisregiii.saude.gov.br"
URL_LOGIN = f"{URL_BASE}/cgi-bin/index"
URL_CONS_ESCALAS = f"{URL_BASE}/cgi-bin/cons_escalas"
URL_AF_MEDICOS = f"{URL_BASE}/cgi-bin/af_medicos.pl"
URL_CONFIG_PREPARO = f"{URL_BASE}/cgi-bin/config_preparo"
URL_GERENCIADOR_SOLICITACAO = f"{URL_BASE}/cgi-bin/gerenciador_solicitacao"
URL_AUTORIZADOR = f"{URL_BASE}/cgi-bin/autorizador"

# ---------------------------------------------------------------------------
# Cabecalhos HTTP
# ---------------------------------------------------------------------------

# Cabecalho Firefox realista. Mantido identico ao sisreg_afastamentos/preparos
# (fonte mais recente: Firefox 147 no Linux). O User-Agent nunca deve ser
# alterado sem testar em staging - e o principal sinal de fingerprint.
CABECALHOS_HTTP = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0"),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Priority": "u=0 i",
}

# ---------------------------------------------------------------------------
# Perfis de credencial -> caminhos Infisical
# ---------------------------------------------------------------------------

# Mapa: nome do perfil -> (caminho_infisical, chave_usuario, chave_senha).
# "padrao" e usado por escalas, preparos, solicitacoes.
# "regulador" e usado por afastamentos e fila_vagas.
PERFIS_CREDENCIAL = {
    "padrao": {
        "caminho": "/sisreg",
        "chave_usuario": "SISREG_USER",
        "chave_senha": "SISREG_PASSWORD",
    },
    "regulador": {
        "caminho": "/sisreg_regulacao",
        "chave_usuario": "SISREG_USER",
        "chave_senha": "SISREG_PASSWORD",
    },
}

# ---------------------------------------------------------------------------
# Perfis de politeness (anti-ban)
# ---------------------------------------------------------------------------

# Valores conservadores derivados dos flows legados (8.5 s base + 1 s janela).
# Alterar apenas com evidencia de producao; aumentar, nunca diminuir sem teste.
DELAY_MINIMO_S = 8.5  # segundos entre requisicoes
DELAY_MAXIMO_S = 9.5  # limite superior do jitter
TIMEOUT_LOGIN_S = 30  # timeout para POST de login
TIMEOUT_CONSULTA_S = 180  # timeout para paginas de consulta pesadas

# Numero maximo de tentativas por item antes de registrar falha e seguir em frente.
# Nao usar valores altos (ex.: 100 do legado) - grinding aumenta risco de ban.
MAX_TENTATIVAS_ITEM = 3

# Fator multiplicador de backoff entre tentativas (backoff exponencial suave).
FATOR_BACKOFF = 2.0

# ---------------------------------------------------------------------------
# SLA de frescor por conjunto (dias maximos sem execucao bem-sucedida)
# ---------------------------------------------------------------------------

# Usado pelo monitor de frescor para disparar alerta quando o SLA e violado.
SLA_FRESCOR_DIAS = {
    "escalas": 2,
    "afastamentos": 2,
    "preparos": 8,
    "solicitacoes": 2,
    "fila_vagas": 2,
}

# ---------------------------------------------------------------------------
# Configuracoes de status de solicitacoes (para o extrator de solicitacoes)
# ---------------------------------------------------------------------------

# Mapa status_coluna -> (tipo_periodo, codigo_situacao) para o gerenciador.
# Derivado de sisreg_solicitacoes/constants.py RUN_CONFIGS.
CONFIGS_SOLICITACOES = {
    "AUTORIZADO": {"tipo_pedido": "A", "codigo_situacao": "7"},
    "EXECUTADO": {"tipo_pedido": "E", "codigo_situacao": "7"},
    "PENDENTE": {"tipo_pedido": "S", "codigo_situacao": "1"},
    "CANCELADO": {"tipo_pedido": "S", "codigo_situacao": "3"},
    "DEVOLVIDO": {"tipo_pedido": "S", "codigo_situacao": "4"},
    "REENVIADO": {"tipo_pedido": "S", "codigo_situacao": "5"},
    "NEGADO": {"tipo_pedido": "S", "codigo_situacao": "6"},
}
