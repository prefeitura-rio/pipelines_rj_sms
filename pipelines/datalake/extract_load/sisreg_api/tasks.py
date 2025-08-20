# -*- coding: utf-8 -*-
"""
Tarefas
"""

from datetime import datetime, timedelta
from typing import Any, Dict

# Geral
from uuid import uuid4

import pandas as pd
from elasticsearch import Elasticsearch, exceptions
from prefect.engine.signals import SKIP

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


# Funções auxiliares
def _processar_registro(registro: Dict[str, Any]) -> Dict[str, Any]:
    fonte = registro.get("_source", {})
    return {**fonte}


def _connect_elasticsearch(host, port, scheme, user, password):
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
        log("Falha na conexão com o Elasticsearch: " + str(e))
        raise
    return es


def _build_query(page_size, filters, data_inicial, data_final):
    return {
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


def _initial_search_with_retry(es, index_name, query, scroll_timeout):
    resposta = es.search(index=index_name, body=query, scroll=scroll_timeout)

    # Timeout handling simples na inicial
    retry_count = 0
    while resposta.get("timed_out", False):
        retry_count += 1
        if retry_count >= 5:
            raise RuntimeError("Timeout repetido na consulta inicial.")
        log(f"Tentativa {retry_count}: Timeout na consulta inicial. Retentando...")
        resposta = es.search(index=index_name, body=query, scroll=scroll_timeout)

    # Checa shards
    shards = resposta.get("_shards", {})
    if shards.get("failed", 0) > 0 or shards.get("skipped", 0) > 0:
        raise RuntimeError(f"Busca inicial com falhas em shards: {shards}")

    return resposta


def _extract_first_page_metadata(resposta, data_inicial, data_final):
    scroll_id = resposta.get("_scroll_id")
    total_obj = resposta["hits"]["total"]
    total_registros = total_obj["value"] if total_obj.get("relation") == "eq" else None

    hits = resposta["hits"]["hits"]
    if total_registros == 0 or not hits:
        log(f"Nenhum registro no intervalo {data_inicial} a {data_final}.")
        raise SKIP("Faixa sem registros. Nada a fazer.")

    log(f"Total de registros encontrados ({data_inicial} a {data_final}): {total_registros}")
    return scroll_id, total_registros, hits


def _process_hits(hits):
    return [_processar_registro(reg) for reg in hits]


def _scroll_paginate(es, scroll_id, scroll_timeout, total_registros, dados_processados):
    # Lógica de paginação / scroll
    scroll_ids = [scroll_id] if scroll_id else []
    while True:
        resposta = es.scroll(scroll_id=scroll_id, scroll=scroll_timeout)

        new_scroll_id = resposta.get("_scroll_id")
        if new_scroll_id:
            scroll_ids.append(new_scroll_id)
            scroll_id = new_scroll_id

        shards = resposta.get("_shards", {})
        if shards.get("failed", 0) > 0 or shards.get("skipped", 0) > 0:
            raise RuntimeError(f"Busca inicial com falhas em shards: {shards}")

        hits = resposta["hits"]["hits"]
        if not hits:
            break

        dados_processados.extend([_processar_registro(reg) for reg in hits])
        log(f"Processados {len(dados_processados)}/{total_registros} registros")

    return dados_processados, scroll_ids


def _clear_scroll(es, scroll_ids):
    es.options(ignore_status=(404,)).clear_scroll(scroll_id=scroll_ids)
    log("Scroll encerrado com sucesso.")


def _validate_count_diff(dados_processados, total_registros):
    # Permite até 5% de diferença (mantido)
    diff = abs(len(dados_processados) - total_registros)
    if total_registros > 0 and diff / total_registros > 0.05:
        raise ValueError(
            f"Divergência na contagem de registros processados. "
            f"Esperado: {total_registros}, Obtido: {len(dados_processados)}"
        )


def _to_parquet_with_metadata(dados_processados, run_id, as_of, data_inicial, data_final):
    df = pd.DataFrame(dados_processados)
    df["run_id"] = run_id
    df["as_of"] = as_of

    file_path = f"sisreg_extraction_{data_inicial}_{data_final}.parquet"
    df.to_parquet(file_path, index=False)
    return file_path


# -----------------------------
# Função principal
# -----------------------------
@task
def gera_data_inicial(data_inicio, data_fim):
    """
    Gera a data inicial para o processamento, considerando o intervalo de 6 meses
    caso não seja fornecida.
    Se data_inicio for fornecida, retorna ela.
    """
    if data_inicio == "":
        data_inicio_calculada = pd.to_datetime(data_fim) - timedelta(days=6*30)
    else:
        data_inicio_calculada = data_inicio
    return data_inicio_calculada

@task(max_retries=5, retry_delay=timedelta(seconds=30))
def full_extract_process(
    run_id,
    as_of,
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

    Ao final, escreve em disco em formato Parquet e
    retorna apenas o caminho do arquivo.
    """

    # Conecta ao Elasticsearch
    es = _connect_elasticsearch(host, port, scheme, user, password)

    # Constrói query inicial
    query = _build_query(page_size, filters, data_inicial, data_final)

    # Consulta inicial com regras de retry e checagem de shards
    resposta = _initial_search_with_retry(es, index_name, query, scroll_timeout)

    # Metadados e validações iniciais
    scroll_id, total_registros, hits = _extract_first_page_metadata(
        resposta, data_inicial, data_final
    )

    # Processa batch inicial
    dados_processados = _process_hits(hits)
    log(f"Processados {len(dados_processados)}/{total_registros} registros (lote inicial)")

    # Lógica de paginação / scroll
    dados_processados, scroll_ids = _scroll_paginate(
        es, scroll_id, scroll_timeout, total_registros, dados_processados
    )

    # Limpa scroll
    _clear_scroll(es, scroll_ids)

    # Valida diferença de contagem
    _validate_count_diff(dados_processados, total_registros)

    # DataFrame + Parquet
    file_path = _to_parquet_with_metadata(
        dados_processados, run_id, as_of, data_inicial, data_final
    )

    return file_path


# -----------------------------


@task(nout=2)
def make_run_meta():
    """Gera metadados da execução."""
    return str(uuid4()), datetime.now().isoformat()


@task
def validate_upload(run_id, as_of, environment, bq_table, bq_dataset, data_inicial, data_final):
    row = {
        "run_id": run_id,
        "as_of": as_of,
        "environment": environment,
        "bq_table": bq_table,
        "bq_dataset": bq_dataset,
        "data_inicial": data_inicial,
        "data_final": data_final,
        "validation_date": datetime.now(),
        "completed": True,
    }
    df = pd.DataFrame([row])
    return df
