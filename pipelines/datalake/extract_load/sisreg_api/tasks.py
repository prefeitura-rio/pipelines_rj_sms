# -*- coding: utf-8 -*-
"""
Tarefas
"""

import os
import sys
from datetime import datetime, timedelta

from typing import Any, Dict, Tuple

import pandas as pd

# Geral
from elasticsearch import Elasticsearch, exceptions

# Internos
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.datalake.utils.tasks import prepare_dataframe_for_upload
from pipelines.utils.tasks import upload_df_to_datalake


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

    file_path = f"sisreg_extraction_{data_inicial}_{data_final}.parquet"
    df.to_parquet(file_path, index=False)

    return file_path

@task
def gerar_faixas_de_data(data_inicial: str, data_final: str, dias_por_faixa: int = 1):
    """
    Gera uma lista de tuplas (inicio, fim) dividindo o intervalo
    entre data_inicial e data_final em blocos de tamanho 'dias_por_faixa'.

    As datas podem ser passadas no formato 'YYYY-MM-DD' ou como datetime.
    Se data_final for a string "now", será convertido para o datetime atual.
    """

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

    log(f"Gerando faixas de datas para processamento em lotes.")
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

    log(f"{len(faixas)} Faixas de datas geradas com sucesso.")
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

@task
def prepare_df_from_disk(file_path: str, flow_name: str, flow_owner: str) -> str:
    """
    Lê um arquivo Parquet do disco, chama a tarefa de preparação
    e salva em outro arquivo. Retorna o caminho do arquivo pronto.
    """
    # Lendo do disco
    df = pd.read_parquet(file_path)

    # Executa a preparação (chamando a task existente)
    df_prepared = prepare_dataframe_for_upload.run(
        df=df,
        flow_name=flow_name,
        flow_owner=flow_owner
    )

    # Salva como outro arquivo Parquet 
    prepared_path = file_path.replace(".parquet", "_prepared.parquet")
    df_prepared.to_parquet(prepared_path, index=False)

    return prepared_path

@task
def upload_from_disk(
    file_path: str,
    table_id: str,
    dataset_id: str,
    partition_column: str,
    source_format: str,
):
    """
    Lê o arquivo Parquet já preparado e faz o upload usando a função/task
    existente 'upload_df_to_datalake', que requer DataFrame em memória.
    """
    # Leitura do parquet
    df = pd.read_parquet(file_path)

    # Chamando a task de upload
    upload_df_to_datalake.run(
        df=df,
        table_id=table_id,
        dataset_id=dataset_id,
        partition_column=partition_column,
        source_format=source_format
    )


@task
def delete_file(file_path: str):
    """
    Deleta o arquivo local para liberar espaço em disco.
    """
    # Comentário em português: removendo o arquivo do disco
    if os.path.exists(file_path):
        os.remove(file_path)

