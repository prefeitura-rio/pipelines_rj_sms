# -*- coding: utf-8 -*-
"""
Extrator de escalas do SISREG.

Substitui sisreg_web (Selenium): a exportacao de escalas e um GET autenticado
simples que retorna CSV com separador ";". Nao ha JavaScript obrigatorio -
o endpoint EXPORTAR_ESCALAS e identico ao usado no Selenium, so que acessado
diretamente via requests. Ref.: sisreg_web/sisreg/sisreg.py:download_escala().

O conjunto escalas tem apenas um item de trabalho (a exportacao completa da
municipalidade). Nao ha janela de datas - o endpoint retorna todos os dados
disponiveis para o IBGE do municipio em um unico CSV.

IMPORTANTE (confirmado no spike EPIC 11): o endpoint EXPORTAR_ESCALAS exporta a
ULTIMA busca executada na sessao. Sem uma etapa LISTAR previa na mesma sessao,
ele retorna 0 bytes. Por isso extrair_item faz LISTAR (aquecimento) antes de
EXPORTAR. O CSV real tem 34 colunas (~245 MB, ~674k linhas) e CONTEM CPF
(coluna "CPF PROFISSIONAL EXEC.") - portanto ha PII; nunca logar linhas.
"""

from io import BytesIO
from typing import Dict

import pandas as pd
import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.common.http import requisicao_educada
from pipelines.datalake.extract_load.sisreg.constants import URL_CONS_ESCALAS
from pipelines.datalake.extract_load.sisreg.errors import (
    ErroEstrutura,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.registry import (
    CONJUNTOS,
    registrar_extrator,
)

# Colunas esperadas vazias = validacao de schema desativada para escalas (o gate
# C24 so valida quando o frozenset e nao-vazio). O palpite antigo de 9 colunas
# estava errado: o spike EPIC 11 mostrou 34 colunas reais no export (COD. ESCALA
# AMBULATORIAL, CPF PROFISSIONAL EXEC., COD. CBO, ...). Mantido vazio ("confiado")
# ate a paridade refinar o conjunto correto - conforme o plano.
COLUNAS_ESPERADAS_ESCALAS: frozenset = frozenset()

# IBGE do municipio do Rio de Janeiro - valor fixo usado no legado.
IBGE_RIO = "330455"


def planejar_trabalho_escalas(credenciais: dict, params: dict) -> dict:
    """Retorna o item de trabalho (exportacao completa da municipalidade).

    Escalas nao tem fan-out: um GET exporta tudo de uma vez. O item contem
    os parametros de query que serao passados para extrair_item.
    """
    return {"id": "exportacao_completa", "ibge": IBGE_RIO}


def extrair_item_escalas(
    sessao: requests.Session,
    item: dict,
    params: dict,
) -> Dict[str, pd.DataFrame]:
    """Faz LISTAR (aquecimento) + EXPORTAR_ESCALAS e retorna o CSV como DataFrame.

    O endpoint EXPORTAR_ESCALAS exporta a ULTIMA busca da sessao. Por isso
    executamos LISTAR antes (mesma sessao) para registrar a busca; sem isso o
    export volta 0 bytes (confirmado no spike EPIC 11). A sessao deve estar
    autenticada.

    Args:
        sessao: Sessao requests autenticada no SISREG.
        item: Dict com 'ibge' (codigo IBGE do municipio).
        params: Parametros de fluxo (janela_dias, etc.).

    Returns:
        {"escalas": DataFrame} com as colunas do CSV normalizadas.
    """
    ibge = item.get("ibge", IBGE_RIO)

    base_query = {
        "radioFiltro": "cpf",
        "status": "",
        "dataInicial": "",
        "dataFinal": "",
        "qtd_itens_pag": "50",
        "pagina": "",
        "ibge": ibge,
        "ordenacao": "",
        "clas_lista": "ASC",
        "coluna": "",
    }

    # Aquecimento: LISTAR registra a busca na sessao. O conteudo e descartado -
    # so precisamos que o servidor guarde os filtros antes de exportar.
    log(f"[escalas] LISTAR (aquecimento) para ibge={ibge}")
    requisicao_educada(
        sessao,
        URL_CONS_ESCALAS,
        params={**base_query, "etapa": "LISTAR"},
        conjunto="escalas",
        item="aquecimento_listar",
    )

    query = {**base_query, "etapa": "EXPORTAR_ESCALAS"}

    log(f"[escalas] Iniciando exportacao EXPORTAR_ESCALAS para ibge={ibge}")
    # requisicao_educada: jitter sleep + deteccao de 403/429/CAPTCHA/logout.
    # Mesmo para um unico request, a deteccao de bloqueio e essencial.
    resposta = requisicao_educada(
        sessao, URL_CONS_ESCALAS, params=query, conjunto="escalas", item="exportacao_completa"
    )

    conteudo = resposta.content
    if not conteudo:
        raise ErroVazioSuspeito(
            "Resposta vazia do endpoint EXPORTAR_ESCALAS",
            conjunto="escalas",
            etapa="extracao",
            url=str(resposta.url),
        )

    # Tentar parsear como CSV. O legado usa sep=";".
    try:
        df = pd.read_csv(BytesIO(conteudo), sep=";", encoding="utf-8", dtype=str)
    except Exception as exc:
        raise ErroEstrutura(
            "Falha ao parsear CSV de escalas",
            conjunto="escalas",
            etapa="parsing",
            detalhe=str(exc),
        ) from exc

    if df.empty:
        raise ErroVazioSuspeito(
            "CSV de escalas retornou 0 linhas",
            conjunto="escalas",
            etapa="parsing",
        )

    log(f"[escalas] CSV parseado: {len(df)} linhas, {len(df.columns)} colunas")
    return {"escalas": df}


# ---------------------------------------------------------------------------
# Registro no CONJUNTOS global
# ---------------------------------------------------------------------------

# Atualiza tambem as colunas esperadas do conjunto no registry.
import dataclasses  # noqa: E402 - importacao tardia proposital (evitar ciclo circular)

CONJUNTOS["escalas"] = dataclasses.replace(
    CONJUNTOS["escalas"],
    colunas_esperadas={"escalas": COLUNAS_ESPERADAS_ESCALAS},
)

registrar_extrator(
    chave="escalas",
    planejar_trabalho=planejar_trabalho_escalas,
    extrair_item=extrair_item_escalas,
)
