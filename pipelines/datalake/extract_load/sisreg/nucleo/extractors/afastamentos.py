# -*- coding: utf-8 -*-
"""
Extrator de afastamentos do SISREG.

Substitui sisreg_afastamentos. Correcoes em relacao ao legado:
- Uma unica sessao regulador para TODOS os CPFs (uma sessao por run).
  O legado criava uma sessao por CPF, gerando ~2-3k logins/dia - principal
  risco de ban. Esta versao faz um unico login e reutiliza a sessao.
- Filtro de CPFs ativos nos ultimos 30 dias (~2-3k vs 8115 total, ~10-15h/dia).
- LGPD: CPFs nunca logados em texto plano; usa indice numerico no log.
- TLS verificado (verify=True herdado da sessao comum).
- Requests via requisicao_educada (jitter + deteccao de bloqueio).
- Reautenticacao inline em caso de expiracao de sessao mid-run (D4).

Fontes:
  - CPFs: rj-sms.saude_sisreg.oferta_programada (curado, read-only)
  - Paginas: /cgi-bin/af_medicos.pl?cpf=X&mostrar_antigos=1 (current)
             /cgi-bin/af_medicos.pl?cpf=X&op=Log              (historico)
"""

import re
from datetime import datetime
from io import StringIO
from typing import List, Optional
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.constants import URL_AF_MEDICOS
from pipelines.datalake.extract_load.sisreg.nucleo.auth import requisicao_com_reauth
from pipelines.datalake.extract_load.sisreg.nucleo.errors import (
    ErroBloqueio,
    ErroEstrutura,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.nucleo.resultado import ResultadoConjunto

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


def planejar_trabalho_afastamentos(credenciais: dict, params: dict) -> dict:
    """Retorna o item com a lista completa de CPFs ativos.

    Um unico item garante um unico login por run (anti-ban). O loop
    por CPF ocorre dentro de extrair_item_afastamentos com sessao reusada.
    Levanta ErroVazioSuspeito (via _obter_cpfs_ativos) se nao houver CPFs.
    """
    cpfs = _obter_cpfs_ativos(params)
    return {"id": "todos_os_cpfs", "cpfs": cpfs}


def _parsear_afastamentos_current(
    html: str, cpf: str, data_extracao: datetime
) -> Optional[pd.DataFrame]:
    """Parseia a pagina de afastamentos atuais de um CPF.

    Retorna None se o profissional nao for encontrado ou nao tiver afastamentos.
    """
    if "Profissional nao encontrado!" in html:
        return None

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
    """
    if "Nenhum Log Encontrado" in html:
        return None

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
) -> ResultadoConjunto:
    """Extrai afastamentos de TODOS os CPFs com uma unica sessao reusada.

    Usa a sessao regulador para ambas as paginas de cada CPF. Um unico login
    por run elimina o churn de ~2-3k logins que era o principal risco de ban.

    Args:
        sessao: Sessao requests autenticada com perfil regulador.
        item: Dict com 'cpfs' (lista) e 'id'.
        params: Parametros do fluxo (inclui 'usuario' e 'senha' para re-auth).

    Returns:
        ResultadoConjunto com afastamentos/afastamento_historico e metricas
        de sub-itens (total CPFs, CPFs ok, ids de CPFs que falharam).
    """
    cpfs: List[str] = item["cpfs"]
    usuario = params.get("usuario", "")
    senha = params.get("senha", "")
    data_extracao = datetime.now(_FUSO_SP)

    log(f"[afastamentos] Iniciando loop: {len(cpfs)} CPFs com sessao unica")

    df_atuals: List[pd.DataFrame] = []
    df_hists: List[pd.DataFrame] = []
    ids_falhos: List[str] = []
    ok = 0

    for i, cpf in enumerate(cpfs):
        cpf_idx = f"cpf_{i}"
        try:
            resp_atual = requisicao_com_reauth(
                sessao,
                URL_AF_MEDICOS,
                params={"cpf": cpf, "mostrar_antigos": "1"},
                usuario=usuario,
                senha=senha,
                conjunto="afastamentos",
                item=cpf_idx,
            )
            df_atual = _parsear_afastamentos_current(resp_atual.text, cpf, data_extracao)

            resp_hist = requisicao_com_reauth(
                sessao,
                URL_AF_MEDICOS,
                params={"cpf": cpf, "op": "Log"},
                usuario=usuario,
                senha=senha,
                conjunto="afastamentos",
                item=cpf_idx,
            )
            df_hist = _parsear_historico_afastamentos(resp_hist.text, cpf, data_extracao)

            if df_atual is not None:
                df_atuals.append(df_atual)
            if df_hist is not None:
                df_hists.append(df_hist)
            ok += 1

        except ErroBloqueio:
            # Bloqueio de IP - abort imediato; nao tente mais CPFs
            log(
                f"[afastamentos/{cpf_idx}] BLOQUEIO de IP detectado - abortando run",
                level="error",
            )
            raise

        except Exception as exc:  # noqa: BLE001
            log(
                f"[afastamentos/{cpf_idx}] Erro ao processar: {type(exc).__name__}: {exc}",
                level="warning",
            )
            ids_falhos.append(cpf_idx)

    df_af = pd.concat(df_atuals, ignore_index=True) if df_atuals else pd.DataFrame()
    df_hist_final = pd.concat(df_hists, ignore_index=True) if df_hists else pd.DataFrame()

    log(
        f"[afastamentos] Loop concluido: {ok}/{len(cpfs)} ok, "
        f"{len(ids_falhos)} falhas, {len(df_af)} linhas af, {len(df_hist_final)} linhas hist"
    )

    return ResultadoConjunto(
        tabelas={"afastamentos": df_af, "afastamento_historico": df_hist_final},
        total=len(cpfs),
        ok=ok,
        ids_falhos=ids_falhos,
    )
