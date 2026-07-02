# -*- coding: utf-8 -*-
"""
Extrator de fila e vagas do SISREG.

Substitui sisreg_pendentes_vagas (que usava Elasticsearch). A descoberta
de procedimentos agora le de rj-sms.saude_sisreg.solicitacoes (BigQuery),
eliminando a dependencia de ES e reduzindo a superficie de ataque anti-ban.

Fluxo:
  1. planejar_trabalho: busca procedimentos distintos do BQ (situacao P/R/D,
     excluindo PPI). Retorna UM item com todos os procedimentos (anti-ban:
     1 login/run em vez de 1 login por procedimento).
  2. extrair_item: para cada procedimento na lista:
     a. LISTAR -> obtem qtd_sol e codigo_solicitacao
     b. APLICAR -> obtem vagas por unidade (ou registra 0 se nao houver)

Ref.: sisreg_pendentes_vagas/tasks.py (STEP 1 substituido, STEP 2 mantido).
"""

import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.constants import (
    COLUNA_PARTICAO,
    URL_AUTORIZADOR,
)
from pipelines.datalake.extract_load.sisreg.nucleo.errors import (
    ErroBloqueio,
    ErroEstrutura,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.nucleo.http import requisicao_educada
from pipelines.datalake.extract_load.sisreg.nucleo.resultado import ResultadoConjunto

_FUSO_SP = ZoneInfo("America/Sao_Paulo")

# Substituicao do Elasticsearch: lemos os procedimentos do BQ curado.
# PPI excluido com o mesmo padrao regex do legado ES (ends-with PPI).
# Trust the codes: id_procedimento_sisreg -> autorizador nu_procedimento.
_SQL_PROCEDIMENTOS = """
SELECT
    id_procedimento_grupo,
    ANY_VALUE(procedimento_grupo) AS procedimento_grupo,
    id_procedimento_sisreg,
    ANY_VALUE(procedimento)       AS procedimento
FROM `rj-sms.saude_sisreg.solicitacoes`
WHERE solicitacao_situacao IN ('P', 'R', 'D')
  AND NOT REGEXP_CONTAINS(
        UPPER(TRIM(procedimento)),
        r'(?:[-–]\\s*)?PPI\\s*$'
      )
GROUP BY id_procedimento_grupo, id_procedimento_sisreg
"""

_COLUNAS_VAGAS_DETALHADAS = [
    "id_procedimento_grupo",
    "procedimento_grupo",
    "id_procedimento_sisreg",
    "procedimento",
    "cnes_unidade",
    "nome_unidade",
    "data_vaga",
    "dia_semana",
    "hora_vaga",
    "nome_profissional",
    "tipo",
    "qtd_vagas",
    COLUNA_PARTICAO,
    "dt_hr_extracao",
]


def _obter_procedimentos_bq() -> pd.DataFrame:
    """Busca procedimentos distintos do BigQuery (substitui Elasticsearch).

    Retorna DataFrame com colunas: id_procedimento_grupo, procedimento_grupo,
    id_procedimento_sisreg, procedimento. Levanta ErroVazioSuspeito se vazio.
    """
    log("[fila_vagas] Buscando procedimentos no BigQuery")
    cliente = bigquery.Client()
    df = cliente.query_and_wait(_SQL_PROCEDIMENTOS).to_dataframe()

    if df.empty:
        raise ErroVazioSuspeito(
            "Query de procedimentos retornou 0 resultados",
            conjunto="fila_vagas",
            etapa="planejamento",
        )

    log(f"[fila_vagas] {len(df)} procedimentos encontrados")
    return df


def planejar_trabalho_fila_vagas(credenciais: dict, params: dict) -> dict:
    """Retorna o item com todos os procedimentos do BQ.

    Um unico item garante um unico login por run (anti-ban). O loop por
    procedimento ocorre dentro de extrair_item_fila_vagas com sessao reusada.
    Levanta ErroVazioSuspeito (via _obter_procedimentos_bq) se nao houver dados.
    """
    df = _obter_procedimentos_bq()
    procedimentos = df.to_dict(orient="records")
    return {"id": "procedimentos", "procedimentos": procedimentos}


def _listar_procedimento(
    sessao: requests.Session,
    codigo_interno: str,
) -> Tuple[Optional[int], Optional[str]]:
    """ETAPA LISTAR: retorna (qtd_sol, codigo_solicitacao) ou (None, None).

    (None, None) indica que o procedimento nao existe no autorizador SISREG.
    Segue sisreg_pendentes_vagas/tasks.py:_list_procedimento.
    """
    params = {
        "ETAPA": "LISTAR",
        "programa": "AUTORIZADOR",
        "radio_codProc": "interno",
        "qtd_itens_pag": 10,
        "pg": 0,
        "tplaudo": 0,
        "ordem": 2,
        "nu_procedimento": codigo_interno,
    }

    resposta = requisicao_educada(
        sessao, URL_AUTORIZADOR, params=params, conjunto="fila_vagas", item=codigo_interno
    )
    soup = BeautifulSoup(resposta.text, "html.parser")

    if "INEXISTENTES!" in soup.get_text():
        return None, None

    titulos = soup.find_all("td", class_="td_titulo_tabela")
    if len(titulos) < 3:
        raise ErroEstrutura(
            "Landmark td_titulo_tabela ausente na pagina LISTAR",
            conjunto="fila_vagas",
            etapa="listar",
            item=codigo_interno,
        )

    qtd_sol = int(re.findall(r"\d+", titulos[2].text)[0])
    try:
        codigo_solicitacao = (
            soup.find("form").find_all("table")[2].find_all("tr")[2].find_all("td")[1].text
        )
    except (AttributeError, IndexError) as exc:
        raise ErroEstrutura(
            "Estrutura DOM inesperada ao extrair codigo_solicitacao",
            conjunto="fila_vagas",
            etapa="listar",
            item=codigo_interno,
            detalhe=str(exc),
        ) from exc

    return qtd_sol, codigo_solicitacao


def _aplicar_procedimento(
    sessao: requests.Session,
    codigo_solicitacao: str,
    base_record: dict,
    qtd_sol: int,
) -> Tuple[List[dict], List[dict]]:
    """ETAPA APLICAR: coleta vagas por unidade para um procedimento.

    Retorna (resultados, vagas_detalhadas). Segue
    sisreg_pendentes_vagas/tasks.py:_process_vagas_apply.
    """
    params = {
        "ETAPA": "APLICAR",
        "tipo": "R",
        "status": "A",
        "co_cid": "H539",
        "co_solicitacao": codigo_solicitacao,
        "nu_classificacao_risco": 1,
        "radio_codProc": "interno",
    }

    resposta = requisicao_educada(
        sessao,
        URL_AUTORIZADOR,
        params=params,
        conjunto="fila_vagas",
        item=codigo_solicitacao,
    )
    soup = BeautifulSoup(resposta.text, "html.parser")
    texto_pagina = soup.get_text()

    resultados: List[dict] = []
    vagas_detalhadas: List[dict] = []

    if "NENHUMA VAGA ENCONTRADA" in texto_pagina or "encontra-se autorizada." in texto_pagina:
        resultados.append(
            {
                **base_record,
                "cnes_unidade": None,
                "nome_unidade": None,
                "qtd_pend": qtd_sol,
                "qtd_vagas": 0,
            }
        )
        return resultados, vagas_detalhadas

    unidades = soup.select(".td_titulo_tabela")
    for k, unidade in enumerate(unidades):
        unidade_nome = unidade.text.strip().split(" - ")[0]
        div = soup.find("div", id=f"divUnidade{k}")
        if not div:
            continue
        input_tag = div.find("input")
        if not input_tag:
            continue
        unidade_cnes = input_tag["value"].split("|")[3]

        for row in div.find_all("td"):
            texto = row.get_text(strip=True)
            if "-" not in texto:
                continue
            row_data = [x.strip() for x in texto.split("-")]
            if len(row_data) < 6:
                continue
            vagas_detalhadas.append(
                {
                    **base_record,
                    "cnes_unidade": unidade_cnes,
                    "nome_unidade": unidade_nome,
                    "data_vaga": row_data[0],
                    "dia_semana": row_data[1],
                    "hora_vaga": row_data[2],
                    "nome_profissional": row_data[3],
                    "tipo": row_data[4],
                    "qtd_vagas": row_data[5].replace("Vagas:", "").strip(),
                }
            )

        vagas = [int(b.text) for b in div.find_all("b") if b.text.isnumeric()]
        resultados.append(
            {
                **base_record,
                "cnes_unidade": unidade_cnes,
                "nome_unidade": unidade_nome,
                "qtd_pend": qtd_sol,
                "qtd_vagas": sum(vagas),
            }
        )

    return resultados, vagas_detalhadas


def _processar_procedimento(
    sessao: requests.Session,
    proc: Dict[str, Any],
    agora: datetime,
) -> Tuple[List[dict], List[dict]]:
    """LISTAR + APLICAR para um unico procedimento. Retorna (resumo, detalhe)."""
    # Trust the codes, not the names (R do design)
    codigo_interno = str(proc["id_procedimento_sisreg"])
    base_record = {
        "id_procedimento_grupo": proc.get("id_procedimento_grupo"),
        "procedimento_grupo": proc.get("procedimento_grupo"),
        "id_procedimento_sisreg": codigo_interno,
        "procedimento": proc.get("procedimento"),
        COLUNA_PARTICAO: agora.strftime("%Y-%m-%d"),
        "dt_hr_extracao": agora.isoformat(),
    }

    log(f"[fila_vagas] LISTAR codigo={codigo_interno}")
    qtd_sol, codigo_solicitacao = _listar_procedimento(sessao, codigo_interno)

    if qtd_sol is None:
        # Procedimento nao existe no autorizador - registra e continua
        return (
            [
                {
                    **base_record,
                    "cnes_unidade": None,
                    "nome_unidade": None,
                    "qtd_pend": None,
                    "qtd_vagas": None,
                }
            ],
            [],
        )

    return _aplicar_procedimento(sessao, codigo_solicitacao, base_record, qtd_sol)


def extrair_item_fila_vagas(
    sessao: requests.Session,
    item: dict,
    params: dict,
) -> ResultadoConjunto:
    """Extrai fila e vagas de TODOS os procedimentos com uma unica sessao.

    Um unico login por run em vez de 1 login por procedimento. A sessao
    e reusada para todas as chamadas LISTAR+APLICAR de cada procedimento.

    Args:
        sessao: Sessao requests autenticada com perfil regulador.
        item: Dict com 'procedimentos' (lista de dicts do BQ) e 'id'.
        params: Parametros do fluxo.

    Returns:
        ResultadoConjunto com fila_e_vagas/vagas_detalhadas e metricas reais.
    """
    procedimentos: List[Dict[str, Any]] = item["procedimentos"]
    agora = datetime.now(_FUSO_SP)

    log(f"[fila_vagas] Iniciando loop: {len(procedimentos)} procedimentos")

    resumos: List[dict] = []
    detalhes: List[dict] = []
    ids_falhos: List[str] = []
    ok = 0

    for proc in procedimentos:
        codigo_interno = str(proc.get("id_procedimento_sisreg", "?"))
        try:
            res, det = _processar_procedimento(sessao, proc, agora)
            resumos.extend(res)
            detalhes.extend(det)
            ok += 1
        except ErroBloqueio:
            log(
                f"[fila_vagas/{codigo_interno}] BLOQUEIO - abortando run",
                level="error",
            )
            raise
        except Exception as exc:  # noqa: BLE001
            log(
                f"[fila_vagas/{codigo_interno}] Erro: {type(exc).__name__}: {exc}",
                level="warning",
            )
            ids_falhos.append(codigo_interno)

    df_resumo = pd.DataFrame(resumos) if resumos else pd.DataFrame()
    df_detalhe = (
        pd.DataFrame(detalhes) if detalhes else pd.DataFrame(columns=_COLUNAS_VAGAS_DETALHADAS)
    )

    log(
        f"[fila_vagas] Loop concluido: {ok}/{len(procedimentos)} ok, "
        f"{len(ids_falhos)} falhas, {len(df_resumo)} linhas resumo"
    )

    return ResultadoConjunto(
        tabelas={"fila_e_vagas": df_resumo, "vagas_detalhadas": df_detalhe},
        total=len(procedimentos),
        ok=ok,
        ids_falhos=ids_falhos,
    )
