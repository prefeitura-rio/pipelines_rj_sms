# -*- coding: utf-8 -*-
"""
Tarefas
"""

import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Tuple

import pandas as pd
from dateutil import rrule

# Geral
from elasticsearch import Elasticsearch, exceptions

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


def processar_registro(registro: Dict[str, Any]) -> Dict[str, Any]:
    fonte = registro.get("_source", {})
    return {**fonte}


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def full_extract_process(
    host,
    port,
    scheme,
    user,
    password,
    index_name,
    page_size,
    scroll_timeout,
    filters,
    data_inicial,
    data_final,
):
    """
    Extrai dados do SISREG via Elasticsearch API,
    considerando apenas o intervalo [data_inicial, data_final].
    """

    # Conecta ao Elasticsearch
    es = Elasticsearch(
        hosts=[{"host": host, "port": port, "scheme": scheme}],
        basic_auth=(user, password),
        request_timeout=600,
        max_retries=5,
        retry_on_timeout=True,
        http_compress=True,
    )
    try:
        es.info()
        log("Conexão com o Elasticsearch estabelecida.")
    except exceptions.ConnectionError as e:
        log("Falha na conexão com o Elasticsearch:", str(e))
        sys.exit(1)

    # Constrói query inicial
    query = {
        "size": page_size,
        "query": {
            "bool": {
                "must": [
                    {"match": filters},
                    {
                        "range": {
                            "data_solicitacao": {
                                "gte": data_inicial,
                                "lte": data_final,
                                "time_zone": "-03:00",
                            }
                        }
                    },
                ]
            }
        },
    }

    # Consulta inicial
    resposta = es.search(index=index_name, body=query, scroll=scroll_timeout)
    scroll_id = resposta.get("_scroll_id")
    total_registros = resposta["hits"]["total"]["value"]
    hits = resposta["hits"]["hits"]

    if total_registros == 0:
        log(f"Nenhum registro encontrado no intervalo {data_inicial} a {data_final}.")
        return pd.DataFrame()

    log(f"Total de registros encontrados ({data_inicial} a {data_final}): {total_registros}")

    # Processa batch inicial
    dados_processados = [processar_registro(reg) for reg in hits]
    log(f"Processados {len(dados_processados)}/{total_registros} registros (lote inicial)")

    # Lógica de paginação / scroll
    scroll_ids = [scroll_id] if scroll_id else []
    while len(dados_processados) < total_registros:
        resposta = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)
        new_scroll_id = resposta.get("_scroll_id")
        if new_scroll_id:
            scroll_ids.append(new_scroll_id)
            scroll_id = new_scroll_id
        hits = resposta["hits"]["hits"]
        if not hits:
            break
        dados_processados.extend([processar_registro(reg) for reg in hits])
        log(f"Processados {len(dados_processados)}/{total_registros} registros")

    # Limpa scroll
    es.options(ignore_status=(404,)).clear_scroll(scroll_id=scroll_ids)
    log("Scroll encerrado com sucesso.")

    if len(dados_processados) != total_registros:
        log(
            f"Advertência: Divergência na contagem de registros processados. "
            f"Esperado: {total_registros}, Obtido: {len(dados_processados)}"
        )
        return pd.DataFrame()

    df = pd.DataFrame(dados_processados)
    return df


@task
def gerar_faixas_de_data(data_inicial: str, data_final: str, dias_por_faixa: int = 1):
    """
    Gera uma lista de tuplas (inicio, fim) dividindo o intervalo
    entre data_inicial e data_final em blocos de tamanho 'dias_por_faixa'.

    As datas podem ser passadas no formato 'YYYY-MM-DD' ou como datetime.
    Se data_final for a string "now", será convertido para o datetime atual.
    """
    from datetime import datetime, timedelta

    # Verifica e converte data_inicial
    if isinstance(data_inicial, datetime):
        dt_inicial = data_inicial
    else:
        dt_inicial = datetime.fromisoformat(data_inicial[:10])

    # Verifica e converte data_final
    if isinstance(data_final, datetime):
        dt_final = data_final
    else:
        if data_final.lower() == "now":
            dt_final = datetime.now()
        else:
            dt_final = datetime.fromisoformat(data_final[:10])

    faixas = []
    # Cria faixas de datas usando intervalos de 'dias_por_faixa'
    dt_atual = dt_inicial
    while dt_atual <= dt_final:
        dt_chunk_inicio = dt_atual
        dt_chunk_fim = dt_chunk_inicio + timedelta(days=dias_por_faixa - 1)
        if dt_chunk_fim > dt_final:
            dt_chunk_fim = dt_final
        faixa_inicio_str = dt_chunk_inicio.strftime("%Y-%m-%d")
        faixa_fim_str = dt_chunk_fim.strftime("%Y-%m-%d")
        faixas.append((faixa_inicio_str, faixa_fim_str))
        dt_atual = dt_chunk_fim + timedelta(days=1)
    return faixas


@task
def extrair_inicio(faixa: Tuple[str, str]) -> str:
    """
    Extrai o início do intervalo da tupla.
    """
    return faixa[0]


@task
def extrair_fim(faixa: Tuple[str, str]) -> str:
    """
    Extrai o fim do intervalo da tupla.
    """
    return faixa[1]
