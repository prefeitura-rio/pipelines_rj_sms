# -*- coding: utf-8 -*-
"""
Extrator de afastamentos do SISREG.

Substitui sisreg_afastamentos. Correcoes em relacao ao legado:
- Uma unica sessao regulador para ambas as paginas (current + historico).
  O legado criava duas sessoes com identidades trocadas por engano.
- Filtro de CPFs ativos nos ultimos 30 dias (~2-3k vs 8115 total, ~10-15h/dia).
- LGPD: CPFs nunca logados em texto plano; usa indice numerico no log.
- TLS verificado (verify=True herdado da sessao comum).

Fontes:
  - CPFs: rj-sms.saude_sisreg.oferta_programada (curado, read-only)
  - Paginas: /cgi-bin/af_medicos.pl?cpf=X&mostrar_antigos=1 (current)
             /cgi-bin/af_medicos.pl?cpf=X&op=Log              (historico)
"""

import re
from datetime import datetime
from io import StringIO
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.constants import URL_AF_MEDICOS
from pipelines.datalake.extract_load.sisreg.errors import (
    ErroEstrutura,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.registry import registrar_extrator

_FUSO_SP = ZoneInfo("America/Sao_Paulo")

# SQL para buscar CPFs de profissionais ativos nos ultimos 30 dias.
# Filtra por vigencia ativa: evita processar todos os 8115 CPFs (~40h/dia).
# Colunas confirmadas como DATE pelo usuario (sem PARSE_DATE necessario).
_SQL_CPFS = """
SELECT DISTINCT profissional_executante_cpf
FROM `rj-sms.saude_sisreg.oferta_programada`
WHERE procedimento_vigencia_inicial_data <= CURRENT_DATE('America/Sao_Paulo')
  AND procedimento_vigencia_final_data >= DATE_SUB(
        CURRENT_DATE('America/Sao_Paulo'), INTERVAL {dias} DAY
      )
"""

_DIAS_FILTRO_CPF = 30


def _obter_cpfs_ativos(params: dict) -> List[str]:
    """Busca CPFs de profissionais com vigencia ativa no BigQuery.

    Retorna lista de CPFs como strings. Levanta ErroVazioSuspeito se a
    lista estiver vazia (indica falha upstream, nunca benigno).
    """
    dias = params.get("dias_filtro_cpf", _DIAS_FILTRO_CPF)
    sql = _SQL_CPFS.format(dias=dias)

    log("[afastamentos] Consultando CPFs ativos no BigQuery")
    cliente = bigquery.Client()
    df = cliente.query_and_wait(sql).to_dataframe()

    if df.empty:
        raise ErroVazioSuspeito(
            "Query de CPFs retornou 0 resultados - suspeito de falha upstream",
            conjunto="afastamentos",
            etapa="planejamento",
            detalhe=sql[:200],
        )

    cpfs = df["profissional_executante_cpf"].dropna().astype(str).tolist()
    log(f"[afastamentos] {len(cpfs)} CPFs ativos encontrados")
    return cpfs


def planejar_trabalho_afastamentos(credenciais: dict, params: dict) -> List[dict]:
    """Retorna um item por CPF ativo.

    Cada item contem o CPF e o indice para log sem PII.
    """
    cpfs = _obter_cpfs_ativos(params)
    return [{"cpf": cpf, "cpf_idx": f"cpf_{i}"} for i, cpf in enumerate(cpfs)]


def _parsear_afastamentos_current(
    html: str, cpf: str, data_extracao: datetime
) -> Optional[pd.DataFrame]:
    """Parseia a pagina de afastamentos atuais de um CPF.

    Retorna None se o profissional nao for encontrado ou nao tiver afastamentos.
    Segue a logica de sisreg_afastamentos/tasks.py:search_afastamento.
    """
    if "Profissional nao encontrado!" in html:
        return None
    if "(CAPTCHA)" in html:
        from pipelines.datalake.extract_load.sisreg.errors import ErroBloqueio

        raise ErroBloqueio("SISREG solicitou CAPTCHA", conjunto="afastamentos", etapa="parsing")

    soup = BeautifulSoup(html, "lxml")
    tabelas = soup.find_all(class_="table_listagem")

    if len(tabelas) <= 1:
        return None  # Sem afastamentos

    html_table = tabelas[1]
    df = pd.read_html(StringIO(str(html_table)))[0]
    df.columns = df.iloc[1]
    df = (
        df.drop([0, 1])
        .reset_index(drop=True)
        .dropna(axis=1, how="all")
        .rename(
            lambda col: (
                re.sub(r"[^a-zA-Z0-9]+", "_", col)
                .replace("Dt__", "data_")
                .replace("Hr__", "hora_")
                .replace("Cod", "codigo_afastamento")
                .lower()
                .strip("_")
            ),
            axis=1,
        )
    )

    df["cpf"] = cpf
    df["data_extracao"] = data_extracao.strftime("%Y-%m-%d")
    return df


def _parsear_historico_afastamentos(
    html: str, cpf: str, data_extracao: datetime
) -> Optional[pd.DataFrame]:
    """Parseia a pagina de historico de afastamentos de um CPF.

    Retorna None se nenhum log for encontrado.
    Segue a logica de sisreg_afastamentos/tasks.py:search_historico_afastamento.
    """
    if "Nenhum Log Encontrado" in html:
        return None
    if "(CAPTCHA)" in html:
        from pipelines.datalake.extract_load.sisreg.errors import ErroBloqueio

        raise ErroBloqueio("SISREG solicitou CAPTCHA", conjunto="afastamentos", etapa="parsing")

    soup = BeautifulSoup(html, "lxml")
    tabela = soup.find(class_="table_listagem")
    if not tabela:
        raise ErroEstrutura(
            "table_listagem ausente na pagina de historico",
            conjunto="afastamentos",
            etapa="parsing",
        )

    df = pd.read_html(StringIO(str(tabela)))[0]
    df.columns = df.iloc[1]
    df = (
        df.drop([0, 1])
        .reset_index(drop=True)
        .rename(
            lambda col: (
                re.sub(r"[^a-zA-Z0-9]+", "_", str(col))
                .replace("Codigo", "codigo_afastamento")
                .lower()
                .strip("_")
            ),
            axis=1,
        )
    )

    df["cpf"] = cpf
    df["data_extracao"] = data_extracao.strftime("%Y-%m-%d")
    return df


def extrair_item_afastamentos(
    sessao: requests.Session,
    item: dict,
    params: dict,
) -> Dict[str, pd.DataFrame]:
    """Extrai afastamentos atual e historico para um CPF com uma unica sessao.

    Usa a sessao regulador para ambas as paginas - o legado usava duas sessoes
    com identidades trocadas; aqui uma unica sessao elimina o risco de colisao.

    Args:
        sessao: Sessao requests autenticada com perfil regulador.
        item: Dict com 'cpf' e 'cpf_idx' (para log sem PII).
        params: Parametros do fluxo.

    Returns:
        {"afastamentos": df_current, "afastamento_historico": df_historico}
        DataFrames podem estar vazios se o CPF nao tiver registros.
    """
    cpf = item["cpf"]
    cpf_idx = item.get("cpf_idx", "cpf_?")
    data_extracao = datetime.now(_FUSO_SP)

    log(f"[afastamentos] Processando {cpf_idx}")

    # Pagina de afastamentos atuais
    resp_atual = sessao.get(
        URL_AF_MEDICOS,
        params={"cpf": cpf, "mostrar_antigos": "1"},
        timeout=180,
    )
    resp_atual.raise_for_status()
    df_atual = _parsear_afastamentos_current(resp_atual.text, cpf, data_extracao)

    # Pagina de historico (mesma sessao, segundo request)
    resp_hist = sessao.get(
        URL_AF_MEDICOS,
        params={"cpf": cpf, "op": "Log"},
        timeout=180,
    )
    resp_hist.raise_for_status()
    df_hist = _parsear_historico_afastamentos(resp_hist.text, cpf, data_extracao)

    return {
        "afastamentos": df_atual if df_atual is not None else pd.DataFrame(),
        "afastamento_historico": df_hist if df_hist is not None else pd.DataFrame(),
    }


# ---------------------------------------------------------------------------
# Registro no CONJUNTOS global
# ---------------------------------------------------------------------------

registrar_extrator(
    chave="afastamentos",
    planejar_trabalho=planejar_trabalho_afastamentos,
    extrair_item=extrair_item_afastamentos,
)
