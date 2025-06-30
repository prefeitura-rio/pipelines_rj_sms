# -*- coding: utf-8 -*-
"""
Tarefas
"""

from datetime import datetime, timedelta
from typing import Any, Dict
import gc

import pandas as pd
from pymongo import MongoClient, ASCENDING, DESCENDING
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import upload_df_to_datalake
from pipelines.datalake.utils.tasks import prepare_dataframe_for_upload

@task(max_retries=5, retry_delay=timedelta(minutes=3))
def dump_collection_slices_to_datalake(
    flow_name: str,
    flow_owner: str,
    host: str,
    port: int,
    user: str,
    password: str,
    authsource: str,
    db_name: str,
    collection_name: str,
    query: Dict[str, Any] | None,
    slice_var: str,
    slice_size: int,
    bq_dataset_id: str,
    bq_table_id: str,
) -> int:
    """Faz a extração paginada da *collection* MongoDB utilizando **slice_var**
    como critério e envia cada fatia diretamente ao Data Lake.

    Nenhuma fatia permanece em memória após o upload.

    Retorna o total de documentos processados.
    """

    log("Conectando ao MongoDB...")
    conn_str = f"mongodb://{user}:{password}@{host}:{port}/?authSource={authsource}"
    client = MongoClient(conn_str)
    log("Conexão com o MongoDB estabelecida.")
    db = client[db_name]
    log(f"Collection: {collection_name}")
    coll = db[collection_name]

    if query is None:
        query = {}

    # Obtém o total de registros na collection com a query fornecida
    total_registros_real = coll.count_documents(query)
    log(f"Total de registros encontrados na collection: {total_registros_real}")

    # Descobre os limites da variável de corte -----------------------------
    log(f"Obtendo max e min de '{slice_var}'...")
    min_doc = list(
        coll.find(query, {slice_var: 1, "_id": 0}).sort(slice_var, ASCENDING).limit(1)
    )
    max_doc = list(
        coll.find(query, {slice_var: 1, "_id": 0}).sort(slice_var, DESCENDING).limit(1)
    )

    if not min_doc or not max_doc:
        log("Nenhum documento encontrado com a query fornecida. Encerrando.")
        client.close()
        return 0

    min_val = min_doc[0][slice_var]
    max_val = max_doc[0][slice_var]
    log(f"Intervalo detectado: {min_val} → {max_val}")

    total_docs = 0
    current_start = min_val

    log(f"Iniciando extração em fatias de {slice_size} unidades de '{slice_var}'...")
    while current_start <= max_val:
        # Calcula o próximo valor de corte considerando o tipo de current_start
        if isinstance(current_start, (int, float)):
            current_end = current_start + slice_size
        elif isinstance(current_start, pd.Timestamp):
            current_end = current_start + pd.Timedelta(slice_size, unit="D")
        elif isinstance(current_start, datetime):
            current_end = current_start + timedelta(days=slice_size)
        else:
            raise TypeError(f"Tipo de variável de fatiamento '{type(current_start)}' não suportado.")

        slice_query = {
            **query,
            slice_var: {"$gte": current_start, "$lt": current_end},
        }

        log(f"Consultando documentos entre {current_start} e {current_end}…")
        docs = list(coll.find(slice_query))
        log(f"Processando fatia de {current_start} a {current_end}…")

        if docs:
            total_docs += len(docs)

            # Converte para DataFrame -------------------------------------
            df = pd.json_normalize(docs)
            if "_id" in df.columns:
                df["_id"] = df["_id"].astype(str)

            log("Preparando DataFrame para upload…")
            df_prepared = prepare_dataframe_for_upload.run(
                df=df, flow_name=flow_name, flow_owner=flow_owner
            )

            # Upload para o Data Lake -----------------------------------
            log("Enviando fatia para o Data Lake…")
            upload_df_to_datalake.run(
                df=df_prepared,
                table_id=bq_table_id,
                dataset_id=bq_dataset_id,
                partition_column="data_extracao",
                source_format="parquet",
            )

            # Libera recursos -------------------------------------------
            del df, df_prepared, docs
            gc.collect()
            log(f"Fatia {current_start}-{current_end} processada e enviada para o DataLake.")

        current_start = current_end

    client.close()

    if total_docs == total_registros_real:
        log(f"Processo concluído. {total_docs} documentos enviados ao Data Lake.")
    else:
        log(f"Erro: {total_docs} documentos processados, mas {total_registros_real} documentos foram encontrados na collection. Interrompendo o processo.")
        raise RuntimeError(f"Número de documentos processados ({total_docs}) difere do total encontrado ({total_registros_real}).")

    return total_docs