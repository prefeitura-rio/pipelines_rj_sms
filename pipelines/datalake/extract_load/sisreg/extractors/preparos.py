# -*- coding: utf-8 -*-
"""
Extrator de preparos do SISREG.

Substitui sisreg_preparos. Correcoes em relacao ao legado:
- Remove DEFAULT_UNIDADES_LIMIT=2 (hardcoded no legado, impedia extracao completa).
- Sleep incorporado via requisicao_educada (nao mais time.sleep(8.5) manual).
- Separacao de responsabilidades: parse de preparo extraido para funcao propria.

Fluxo: listar unidades -> para cada unidade, listar procedimentos ->
       para cada procedimento, buscar <textarea id=preparo>.

Paginacao (spike EPIC 11): sao ~424 unidades com dezenas de procedimentos cada,
o que tornaria o scrape completo inviavel no ritmo educado (dias). Por isso
preparos e PAGINADO por shard: cada execucao processa apenas um subconjunto
deterministico das unidades (unidades[i] onde i % num_shards == shard), com o
shard derivado da data do run. Ao longo de um ciclo de num_shards execucoes,
todas as unidades sao cobertas. A escrita e "append" (nao overwrite) para nao
apagar os shards anteriores; a deduplicacao "ultima versao por unidade" fica na
camada de modelagem.
"""

from datetime import datetime
from typing import Dict
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.common.http import requisicao_educada
from pipelines.datalake.extract_load.sisreg.constants import URL_CONFIG_PREPARO
from pipelines.datalake.extract_load.sisreg.errors import ErroEstrutura
from pipelines.datalake.extract_load.sisreg.registry import registrar_extrator

_FUSO_SP = ZoneInfo("America/Sao_Paulo")

# Numero de shards padrao: em quantas execucoes o conjunto completo de unidades
# e coberto. Com ~424 unidades e 30 shards, ~14 unidades/run. Ajustavel por
# param 'num_shards_preparos'. O ciclo completo leva num_shards execucoes.
_NUM_SHARDS_PADRAO = 30

_ETAPA_PREPARO = "FRM_PREPARO"
_ETAPA_LISTAR_PROCEDIMENTOS = "LISTAR_PROCEDIMENTOS"

# Caracteres substituidos na descricao do preparo (igual ao legado).
_RE_CARACTERES_ESPECIAIS = r"[;►–,]"


def _limpar_descricao(texto: str) -> str:
    """Substitui caracteres especiais na descricao do preparo por ' - '."""
    import re

    return re.sub(_RE_CARACTERES_ESPECIAIS, " - ", texto or "")


def _obter_unidades(sessao: requests.Session) -> list:
    """Retorna a lista de tags <option> das unidades disponiveis.

    Levanta ErroEstrutura se o formulario ou o select nao forem encontrados.
    """
    resposta = requisicao_educada(sessao, URL_CONFIG_PREPARO, conjunto="preparos")
    soup = BeautifulSoup(resposta.text, "lxml")
    form = soup.find("form")
    if not form:
        raise ErroEstrutura(
            "Formulario nao encontrado na pagina de preparos",
            conjunto="preparos",
            etapa="listing_unidades",
        )
    tabela = form.find("table")
    if not tabela:
        raise ErroEstrutura(
            "Tabela de unidades nao encontrada no formulario de preparos",
            conjunto="preparos",
            etapa="listing_unidades",
        )
    rows = tabela.find_all("tr")
    if len(rows) < 2:
        return []
    return rows[1].find_all("option")


def _obter_procedimentos(sessao: requests.Session, valor_unidade: str) -> list:
    """Retorna a lista de tags <option> dos procedimentos de uma unidade."""
    resposta = requisicao_educada(
        sessao,
        URL_CONFIG_PREPARO,
        params={"etapa": _ETAPA_LISTAR_PROCEDIMENTOS, "unidade": valor_unidade},
        conjunto="preparos",
        item=valor_unidade,
    )
    soup = BeautifulSoup(resposta.text, "lxml")
    try:
        procedimentos = soup.form.table("tr")[2]("option")
    except (AttributeError, IndexError):
        return []
    return procedimentos


def _obter_preparo(
    sessao: requests.Session,
    valor_unidade: str,
    nome_unidade: str,
    valor_proc: str,
    nome_proc: str,
) -> dict:
    """Busca o texto de preparo de um procedimento especifico.

    Retorna dict com os campos do registro de preparo.
    """
    resposta = requisicao_educada(
        sessao,
        URL_CONFIG_PREPARO,
        params={
            "etapa": _ETAPA_PREPARO,
            "unidade": valor_unidade,
            "procedimento": valor_proc,
        },
        conjunto="preparos",
        item=f"{valor_unidade}/{valor_proc}",
    )
    soup = BeautifulSoup(resposta.text, "lxml")
    textarea = soup.find("textarea", {"id": "preparo"})
    descricao = textarea.get_text(strip=True) if textarea else ""

    return {
        "cnes": valor_unidade,
        "nome_unidade": nome_unidade,
        "cod_procedimento": valor_proc,
        "nome_procedimento": nome_proc,
        "descricao_preparo": _limpar_descricao(descricao),
    }


def planejar_trabalho_preparos(credenciais: dict, params: dict) -> dict:
    """Retorna o item com o shard de unidades a processar nesta execucao.

    Preparos e paginado: cada run cobre apenas as unidades do shard atual,
    derivado deterministicamente da data (dia ordinal % num_shards). O loop
    interno (unidade -> procedimentos -> preparo) fica em extrair_item.
    """
    num_shards = int(params.get("num_shards_preparos", _NUM_SHARDS_PADRAO))
    if num_shards < 1:
        num_shards = 1
    shard = datetime.now(_FUSO_SP).toordinal() % num_shards
    return {"id": "lote_de_unidades", "shard": shard, "num_shards": num_shards}


def extrair_item_preparos(
    sessao: requests.Session,
    item: dict,
    params: dict,
) -> Dict[str, pd.DataFrame]:
    """Extrai todos os preparos de todas as unidades e procedimentos.

    Loop: lista unidades -> lista procedimentos por unidade ->
    busca <textarea id=preparo> por procedimento.

    Args:
        sessao: Sessao requests autenticada com perfil padrao.
        item: Dict com 'shard' e 'num_shards' (paginacao por unidade).
        params: Parametros do fluxo.

    Returns:
        {"preparos": DataFrame} com os registros das unidades deste shard.
    """
    shard = int(item.get("shard", 0))
    num_shards = int(item.get("num_shards", 1))

    unidades = _obter_unidades(sessao)
    # Seleciona apenas as unidades deste shard (paginacao deterministica).
    lote = [u for i, u in enumerate(unidades) if i % num_shards == shard]
    log(
        f"[preparos] Shard {shard}/{num_shards}: "
        f"{len(lote)} de {len(unidades)} unidades neste run"
    )

    registros: list = []
    for i, unidade in enumerate(lote):
        valor_unidade = unidade.get("value", "")
        nome_unidade = unidade.get_text(strip=True)
        if not valor_unidade:
            continue

        procedimentos = _obter_procedimentos(sessao, valor_unidade)
        log(
            f"[preparos] Unidade {i+1}/{len(lote)} "
            f"({nome_unidade}): {len(procedimentos)} procedimentos"
        )

        # Pula o primeiro option que costuma ser um placeholder vazio
        for proc in procedimentos[1:]:
            valor_proc = proc.get("value", "")
            nome_proc = proc.get_text(strip=True)
            if not valor_proc:
                continue
            registro = _obter_preparo(sessao, valor_unidade, nome_unidade, valor_proc, nome_proc)
            registros.append(registro)

    if not registros:
        log("[preparos] Nenhum preparo coletado - retornando DataFrame vazio")
        return {"preparos": pd.DataFrame()}

    df = pd.DataFrame(registros)
    log(f"[preparos] Total: {len(df)} preparos coletados")
    return {"preparos": df}


# ---------------------------------------------------------------------------
# Registro no CONJUNTOS global
# ---------------------------------------------------------------------------

registrar_extrator(
    chave="preparos",
    planejar_trabalho=planejar_trabalho_preparos,
    extrair_item=extrair_item_preparos,
)
