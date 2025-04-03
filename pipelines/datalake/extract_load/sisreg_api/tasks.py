# -*- coding: utf-8 -*-
"""
Tarefas
"""

import sys
from datetime import timedelta
from typing import Any, Dict

import pandas as pd

# Geral
from elasticsearch import Elasticsearch, exceptions

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task


@task(max_retries=5, retry_delay=timedelta(seconds=30))
def connect_elasticsearch(host, port, scheme, user, password):
    """
    Cria o cliente Elasticsearch com parâmetros de conexão.
    Levanta exceção se a conexão falhar.
    """
    es = Elasticsearch(
        hosts=[{"host": host, "port": port, "scheme": scheme}],
        basic_auth=(user, password),
        request_timeout=600,
        max_retries=5,
        retry_on_timeout=True,
        http_compress=True,
    )
    try:
        # Testa conexão
        es.info()
        log("Conexão com o Elasticsearch estabelecida.")

    except exceptions.ConnectionError as e:
        log("Falha na conexão com o Elasticsearch:", str(e))
        sys.exit(1)

    return es


@task(max_retries=3, retry_delay=timedelta(seconds=30), nout=3)
def iniciar_consulta(
    client, index_name, page_size, scroll_timeout, filters, data_inicial, data_final
):
    """
    Faz a consulta inicial ao Elasticsearch (primeira página).
    Retorna (scroll_id, total_registros, hits_iniciais).
    """
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

    resposta = client.search(index=index_name, body=query, scroll=scroll_timeout)

    scroll_id = resposta.get("_scroll_id")
    total = resposta["hits"]["total"]["value"]
    hits = resposta["hits"]["hits"]

    # Se total_registros for 0, encerra
    if total == 0:
        log("Nenhum registro encontrado. Processo encerrado.")
        return

    log(f"Total de registros encontrados: {total}")
    return scroll_id, total, hits


def processar_registro(registro: Dict[str, Any]) -> Dict[str, Any]:
    fonte = registro.get("_source", {})
    return {**fonte}


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def processar_lote_inicial(hits_iniciais, total_registros):
    """
    Processa o lote inicial de registros.
    """
    dados_processados = [processar_registro(reg) for reg in hits_iniciais]
    log(f"Processados {len(dados_processados)}/{total_registros} registros (lote inicial)")
    return dados_processados


@task(max_retries=3, retry_delay=timedelta(seconds=30), nout=2)
def continuar_scroll_e_processar(
    client, scroll_id, scroll_timeout, total_registros, ja_processados
):
    """
    Continua buscando e processando registros via scroll até que todos os dados sejam recuperados.
    """

    dados_processados = ja_processados
    scroll_ids_usados = [scroll_id] if scroll_id else []

    while len(dados_processados) < total_registros:
        # Realiza a próxima busca com o scroll_id
        resposta = client.scroll(scroll_id=scroll_id, scroll=scroll_timeout)

        # Atualiza o scroll_id
        scroll_id = resposta.get("_scroll_id")
        if scroll_id:
            scroll_ids_usados.append(scroll_id)

        # Obtém os hits (registros retornados)
        hits = resposta["hits"]["hits"]

        # Se não houver mais hits, encerra o loop
        if not hits:
            break

        # Processa os registros retornados
        lote_convertido = [processar_registro(reg) for reg in hits]
        dados_processados.extend(lote_convertido)

        log(f"Processados {len(dados_processados)}/{total_registros} registros")

    # Verifica se todos os registros foram processados
    if len(dados_processados) != total_registros:
        log(
            f"Advertência: Divergência na contagem de registros processados. "
            f"Esperado: {total_registros}, Obtido: {len(dados_processados)}"
        )

    return pd.DataFrame(dados_processados), scroll_ids_usados


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def limpar_scroll(client, scroll_ids):
    """
    Encerra o scroll para cada ID, liberando recursos no Elasticsearch.
    """
    client.options(ignore_status=(404,)).clear_scroll(scroll_id=scroll_ids)
    log("Scroll encerrado com sucesso.")
