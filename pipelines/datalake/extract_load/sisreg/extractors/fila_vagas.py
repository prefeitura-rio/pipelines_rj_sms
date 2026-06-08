# -*- coding: utf-8 -*-
"""
Extrator de fila e vagas do SISREG.

Substitui sisreg_pendentes_vagas (que usava Elasticsearch). A descoberta
de procedimentos agora le de rj-sms.saude_sisreg.solicitacoes (BigQuery),
eliminando a dependencia de ES e reduzindo a superficie de ataque anti-ban.

Fluxo:
  1. planejar_trabalho: busca procedimentos distintos do BQ (situacao P/R/D,
     excluindo PPI). Confia nos codigos, nao nos nomes (nomes podem divergir).
  2. extrair_item: para cada procedimento:
     a. LISTAR -> obtem qtd_sol e codigo_solicitacao
     b. APLICAR -> obtem vagas por unidade (ou registra 0 se nao houver)

Ref.: sisreg_pendentes_vagas/tasks.py (STEP 1 substituido, STEP 2 mantido).
"""

import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg.common.http import requisicao_educada
from pipelines.datalake.extract_load.sisreg.constants import (
    COLUNA_PARTICAO,
    URL_AUTORIZADOR,
)
from pipelines.datalake.extract_load.sisreg.errors import (
    ErroEstrutura,
    ErroVazioSuspeito,
)
from pipelines.datalake.extract_load.sisreg.registry import registrar_extrator

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


def planejar_trabalho_fila_vagas(credenciais: dict, params: dict) -> List[dict]:
    """Retorna um item por procedimento distinto do BQ.

    Cada item contem os campos do procedimento para uso no extrair_item.
    """
    df = _obter_procedimentos_bq()
    items = df.to_dict(orient="records")
    # Adiciona id para log/contexto de erro (codigo, nao nome)
    for item in items:
        item["id"] = str(item.get("id_procedimento_sisreg", "proc_?"))
    return items


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


def extrair_item_fila_vagas(
    sessao: requests.Session,
    item: dict,
    params: dict,
) -> Dict[str, pd.DataFrame]:
    """Extrai fila e vagas para um procedimento (LISTAR + APLICAR).

    Confia nos codigos: id_procedimento_sisreg -> autorizador nu_procedimento.
    Nomes sao labels descritivos apenas (nao usados para match/join).

    Args:
        sessao: Sessao requests autenticada com perfil regulador.
        item: Dict com id_procedimento_sisreg, id_procedimento_grupo, etc.
        params: Parametros do fluxo.

    Returns:
        {"fila_e_vagas": df_resumo, "vagas_detalhadas": df_detalhe}
    """
    # Trust the codes, not the names (R do design)
    codigo_interno = str(item["id_procedimento_sisreg"])
    codigo_grupo = item.get("id_procedimento_grupo")
    nome_grupo = item.get("procedimento_grupo")
    nome_proc = item.get("procedimento")
    agora = datetime.now(_FUSO_SP)

    base_record = {
        "id_procedimento_grupo": codigo_grupo,
        "procedimento_grupo": nome_grupo,
        "id_procedimento_sisreg": codigo_interno,
        "procedimento": nome_proc,
        COLUNA_PARTICAO: agora.strftime("%Y-%m-%d"),
        "dt_hr_extracao": agora.isoformat(),
    }

    log(f"[fila_vagas] LISTAR codigo={codigo_interno}")
    qtd_sol, codigo_solicitacao = _listar_procedimento(sessao, codigo_interno)

    if qtd_sol is None:
        # Procedimento nao existe no autorizador - registra e continua
        df_resumo = pd.DataFrame(
            [
                {
                    **base_record,
                    "cnes_unidade": None,
                    "nome_unidade": None,
                    "qtd_pend": None,
                    "qtd_vagas": None,
                }
            ]
        )
        return {
            "fila_e_vagas": df_resumo,
            "vagas_detalhadas": pd.DataFrame(columns=_COLUNAS_VAGAS_DETALHADAS),
        }

    resultados, vagas_detalhadas = _aplicar_procedimento(
        sessao, codigo_solicitacao, base_record, qtd_sol
    )

    df_resumo = pd.DataFrame(resultados)
    df_detalhe = (
        pd.DataFrame(vagas_detalhadas)
        if vagas_detalhadas
        else pd.DataFrame(columns=_COLUNAS_VAGAS_DETALHADAS)
    )

    return {"fila_e_vagas": df_resumo, "vagas_detalhadas": df_detalhe}


# ---------------------------------------------------------------------------
# Registro no CONJUNTOS global
# ---------------------------------------------------------------------------

registrar_extrator(
    chave="fila_vagas",
    planejar_trabalho=planejar_trabalho_fila_vagas,
    extrair_item=extrair_item_fila_vagas,
)
