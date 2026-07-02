# -*- coding: utf-8 -*-
"""
Extrator de solicitacoes do SISREG.

Substitui sisreg_solicitacoes. Correcoes em relacao ao legado:
- max_retries=100 substituido por MAX_TENTATIVAS_ITEM=3 (politeness).
- Janela rolling de 180 dias (parametrizavel) em vez de mes anterior fixo.
- Um unico item de trabalho com o roteiro completo (anti-ban: 1 login/run).
  O legado gerava 1 item por data+status (1.267 logins/dia para janela=180).
- Reautenticacao inline em caso de expiracao de sessao mid-run (D4).

Ref.: sisreg_solicitacoes/tasks.py e constants.py
"""

from datetime import datetime, timedelta
from typing import List
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.constants import (
    CONFIGS_SOLICITACOES,
    JANELA_DIAS,
    URL_GERENCIADOR_SOLICITACAO,
)
from pipelines.datalake.extract_load.sisreg.nucleo.auth import requisicao_com_reauth
from pipelines.datalake.extract_load.sisreg.nucleo.errors import (
    ErroBloqueio,
    ErroEstrutura,
)
from pipelines.datalake.extract_load.sisreg.nucleo.parsing import (
    tabela_listagem_para_dataframe,
)
from pipelines.datalake.extract_load.sisreg.nucleo.resultado import ResultadoConjunto

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


def planejar_trabalho_solicitacoes(credenciais: dict, params: dict) -> dict:
    """Retorna o item com o roteiro completo de extracao.

    Um unico item garante um unico login por run (anti-ban). O loop sobre
    datas x status ocorre dentro de extrair_item_solicitacoes com sessao reusada.
    """
    janela = int(params.get("janela_dias", JANELA_DIAS))
    roteiro = _gerar_roteiro(janela)
    log(f"[solicitacoes] Roteiro gerado: {len(roteiro)} fichas (janela={janela}d)")
    return {"id": "roteiro", "roteiro": roteiro}


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


def _buscar_ficha(
    sessao: requests.Session,
    ficha: dict,
    usuario: str,
    senha: str,
) -> pd.DataFrame:
    """Busca uma ficha (data+status) e retorna o DataFrame correspondente.

    Lida com LOGOUT fazendo reautenticacao inline e tentando mais uma vez.
    """
    item_id = ficha.get("id", f"{ficha['data']}_{ficha['status_coluna']}")

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
        "tipo_periodo": ficha["tipo_pedido"],
        "dt_inicial": ficha["data"],
        "dt_final": ficha["data"],
        "cmb_situacao": ficha["codigo_situacao"],
        "qtd_itens_pag": "0",
        "co_seq_solicitacao": "",
        "ordenacao": "2",
        "pagina": "0",
    }

    resposta = requisicao_com_reauth(
        sessao,
        URL_GERENCIADOR_SOLICITACAO,
        params=query,
        usuario=usuario,
        senha=senha,
        conjunto="solicitacoes",
        item=item_id,
    )

    status = _verificar_status_pagina(resposta.text)

    if status == "VAZIO":
        return pd.DataFrame()

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
        df["status"] = ficha["status_coluna"]

    return df


def extrair_item_solicitacoes(
    sessao: requests.Session,
    item: dict,
    params: dict,
) -> ResultadoConjunto:
    """Extrai todas as solicitacoes do roteiro com uma unica sessao reusada.

    Percorre o produto cartesiano data x status sobre a janela de 180 dias
    com uma sessao autenticada compartilhada. Um unico login por run em vez
    de ~1.267 logins com a abordagem por-item-mapeado.

    Args:
        sessao: Sessao requests autenticada com perfil padrao.
        item: Dict com 'roteiro' (lista de fichas) e 'id'.
        params: Parametros do fluxo (inclui 'usuario' e 'senha' para re-auth).

    Returns:
        ResultadoConjunto com DataFrame de solicitacoes e metricas reais.
    """
    roteiro: List[dict] = item["roteiro"]
    usuario = params.get("usuario", "")
    senha = params.get("senha", "")

    log(f"[solicitacoes] Iniciando loop: {len(roteiro)} fichas com sessao unica")

    dfs: List[pd.DataFrame] = []
    ids_falhos: List[str] = []
    ok = 0

    for ficha in roteiro:
        item_id = ficha.get("id", "")
        try:
            df = _buscar_ficha(sessao, ficha, usuario, senha)
            dfs.append(df)
            ok += 1
        except ErroBloqueio:
            log(
                f"[solicitacoes/{item_id}] BLOQUEIO - abortando run",
                level="error",
            )
            raise
        except Exception as exc:  # noqa: BLE001
            log(
                f"[solicitacoes/{item_id}] Erro: {type(exc).__name__}: {exc}",
                level="warning",
            )
            ids_falhos.append(item_id)

    df_final = (
        pd.concat([d for d in dfs if not d.empty], ignore_index=True)
        if any(not d.empty for d in dfs)
        else pd.DataFrame()
    )

    log(
        f"[solicitacoes] Loop concluido: {ok}/{len(roteiro)} ok, "
        f"{len(ids_falhos)} falhas, {len(df_final)} linhas"
    )

    return ResultadoConjunto(
        tabelas={"solicitacoes": df_final},
        total=len(roteiro),
        ok=ok,
        ids_falhos=ids_falhos,
    )
