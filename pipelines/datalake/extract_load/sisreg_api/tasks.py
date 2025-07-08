# -*- coding: utf-8 -*-
"""
Tarefas
"""

# Geral
import sys
from datetime import timedelta
from typing import Any, Dict

import pandas as pd
from elasticsearch import Elasticsearch, exceptions
from prefect.engine.signals import SKIP

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message


def processar_registro(registro: Dict[str, Any]) -> Dict[str, Any]:
    fonte = registro.get("_source", {})
    return {**fonte}


def checa_completude(dados_processados: int, total_registros: int):
    """
    Valida se a diferença entre registros esperados e processados
    excede 5 %. Se exceder, dispara erro.

    Parameters
    ----------
    dados_processados : Iterable | pd.DataFrame
        Coleção ou DataFrame já processado.
    total_registros : int
        Quantidade total de registros esperados.
    """

    # Converte para len() de forma segura
    processados = len(dados_processados) if dados_processados is not None else 0
    diff = abs(processados - total_registros)
    porcent_diff = diff / total_registros if total_registros else 1

    # Critério de falha (> 5 %)
    if total_registros == 0 or porcent_diff > 0.05:
        mensagem_erro = (
            f"Divergência na contagem de registros processados — "
            f"Esperado: {total_registros}, Obtido: {processados}"
        )
        log(mensagem_erro)

        send_message(
            title="❌ Erro no Fluxo SISREG API",
            message=mensagem_erro,
            monitor_slug="error",
        )

        raise ValueError(mensagem_erro)

    # Caso passe na verificação
    log(
        f"Completude OK: \
        {processados}/{total_registros} ({porcent_diff * 100:.2f} % de diferença)"
    )


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

    Ao final, escreve em disco em formato Parquet e
    retorna apenas o caminho do arquivo.
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
        "track_total_hits": True,
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
    total_registros = es.count(index=index_name, body={"query": query["query"]})["count"]
    hits = resposta["hits"]["hits"]

    if total_registros == 0:
        log(f"Nenhum registro encontrado no intervalo {data_inicial} a {data_final}.")
        raise SKIP("Faixa sem registros. Nada a fazer.")

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

    checa_completude(dados_processados, total_registros)

    df = pd.DataFrame(dados_processados)

    file_path = f"sisreg_extraction_{data_inicial}_{data_final}.parquet"
    df.to_parquet(file_path, index=False)

    return file_path
