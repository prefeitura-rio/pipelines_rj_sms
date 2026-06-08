# -*- coding: utf-8 -*-
"""
Extrator de solicitacoes do SISREG.

Substitui sisreg_solicitacoes. Correcoes em relacao ao legado:
- max_retries=100 substituido por MAX_TENTATIVAS_ITEM=3 (politeness).
- Janela rolling de 180 dias (parametrizavel) em vez de mes anterior fixo.
- planejar_trabalho gera o produto cartesiano data x status (roteiro).
- extrair_item e atomico: uma data + um status = um fragmento.

Ref.: sisreg_solicitacoes/tasks.py e constants.py
"""

from datetime import datetime, timedelta
from typing import Dict, List
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.common.http import requisicao_educada
from pipelines.datalake.extract_load.sisreg.common.parsing import (
    tabela_listagem_para_dataframe,
)
from pipelines.datalake.extract_load.sisreg.constants import (
    CONFIGS_SOLICITACOES,
    JANELA_DIAS,
    URL_GERENCIADOR_SOLICITACAO,
)
from pipelines.datalake.extract_load.sisreg.errors import ErroEstrutura
from pipelines.datalake.extract_load.sisreg.registry import registrar_extrator

_FUSO_SP = ZoneInfo("America/Sao_Paulo")

# Colunas que identificam a tabela de solicitacoes no HTML
_COLUNAS_OBRIGATORIAS = ("Cód. Solicitação", "Data da Solicitação")


def _gerar_roteiro(janela_dias: int) -> List[dict]:
    """Gera o produto cartesiano data x status para a janela de dias.

    Cada item do roteiro e um dict com 'data' (DD/MM/AAAA) e os campos
    de config do status (tipo_pedido, codigo_situacao, status_coluna).
    """
    hoje = datetime.now(_FUSO_SP).date()
    data_inicial = hoje - timedelta(days=janela_dias)

    roteiro: List[dict] = []
    delta = (hoje - data_inicial).days
    for i in range(delta + 1):
        data = (data_inicial + timedelta(days=i)).strftime("%d/%m/%Y")
        for status_coluna, cfg in CONFIGS_SOLICITACOES.items():
            roteiro.append(
                {
                    "data": data,
                    "tipo_pedido": cfg["tipo_pedido"],
                    "codigo_situacao": cfg["codigo_situacao"],
                    "status_coluna": status_coluna,
                    "id": f"{data}_{status_coluna}",
                }
            )

    return roteiro


def planejar_trabalho_solicitacoes(credenciais: dict, params: dict) -> List[dict]:
    """Retorna o roteiro de extracao: data x status sobre a janela de 180 dias."""
    janela = int(params.get("janela_dias", JANELA_DIAS))
    roteiro = _gerar_roteiro(janela)
    log(f"[solicitacoes] Roteiro gerado: {len(roteiro)} fichas (janela={janela}d)")
    return roteiro


def _verificar_status_pagina(html: str) -> str:
    """Classifica o status da resposta HTML do gerenciador.

    Retorna "OK", "VAZIO", "LOGOUT" ou "ZOMBIE".
    """
    texto_lower = html.lower()
    if any(m in texto_lower for m in ("efetue o logon novamente", "sessao expirada")):
        return "LOGOUT"
    if any(m in texto_lower for m in ("erro nao esperado", "erro inesperado")):
        return "ZOMBIE"
    if "nenhum registro encontrado" in texto_lower:
        return "VAZIO"
    return "OK"


def extrair_item_solicitacoes(
    sessao: requests.Session,
    item: dict,
    params: dict,
) -> Dict[str, pd.DataFrame]:
    """Extrai solicitacoes para uma data e status especificos.

    Usa requisicao_educada (jitter + retry limitado). Reautentica se sessao
    expirou (uma vez). Retorna DataFrame vazio para datas sem registros.

    Args:
        sessao: Sessao requests autenticada com perfil padrao.
        item: Dict com 'data', 'tipo_pedido', 'codigo_situacao', 'status_coluna'.
        params: Parametros do fluxo.

    Returns:
        {"solicitacoes": DataFrame} com as linhas da tabela.
    """
    data = item["data"]
    tipo_pedido = item["tipo_pedido"]
    codigo_situacao = item["codigo_situacao"]
    status_coluna = item["status_coluna"]
    item_id = item.get("id", f"{data}_{status_coluna}")

    query = {
        "etapa": "LISTAR_SOLICITACOES",
        "co_solicitacao": "",
        "cns_paciente": "",
        "no_usuario": "",
        "cnes_solicitante": "",
        "cnes_executante": "",
        "co_proc_unificado": "",
        "co_pa_interno": "",
        "ds_procedimento": "",
        "tipo_periodo": tipo_pedido,
        "dt_inicial": data,
        "dt_final": data,
        "cmb_situacao": codigo_situacao,
        "qtd_itens_pag": "0",
        "co_seq_solicitacao": "",
        "ordenacao": "2",
        "pagina": "0",
    }

    resposta = requisicao_educada(
        sessao,
        URL_GERENCIADOR_SOLICITACAO,
        params=query,
        conjunto="solicitacoes",
        item=item_id,
    )

    status = _verificar_status_pagina(resposta.text)

    if status == "VAZIO":
        return {"solicitacoes": pd.DataFrame()}

    if status in ("LOGOUT", "ZOMBIE"):
        raise ErroEstrutura(
            f"Pagina de solicitacoes em estado inesperado: {status}",
            conjunto="solicitacoes",
            etapa="parsing",
            item=item_id,
        )

    try:
        df = tabela_listagem_para_dataframe(
            resposta.text,
            colunas_obrigatorias=_COLUNAS_OBRIGATORIAS,
            conjunto="solicitacoes",
            item=item_id,
        )
    except Exception as exc:
        raise ErroEstrutura(
            "Falha ao parsear tabela de solicitacoes",
            conjunto="solicitacoes",
            etapa="parsing",
            item=item_id,
            detalhe=str(exc),
        ) from exc

    if not df.empty:
        df["status"] = status_coluna

    return {"solicitacoes": df}


# ---------------------------------------------------------------------------
# Registro no CONJUNTOS global
# ---------------------------------------------------------------------------

registrar_extrator(
    chave="solicitacoes",
    planejar_trabalho=planejar_trabalho_solicitacoes,
    extrair_item=extrair_item_solicitacoes,
)
