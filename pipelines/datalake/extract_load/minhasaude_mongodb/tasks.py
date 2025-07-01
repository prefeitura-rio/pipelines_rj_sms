# -*- coding: utf-8 -*-
"""
Tarefas · Extração paginada de coleção MongoDB -> Carga para o Data Lake (BigQuery)

Objetivo
--------
Extrair documentos em lotes (“fatias”) sem sobrecarregar RAM nem manter cursores
abertos por tempo excessivo, tratando quedas de conexão (`AutoReconnect`) e
subindo cada lote diretamente para o Data Lake.

Fluxo geral
-----------
1. Conecta ao MongoDB com time-outs e keep-alive ajustados.
2. Determina intervalo mínimo/máximo da variável de fatiamento (`slice_var`).
3. Percorre esse intervalo em passos (`slice_size`), criando um cursor por fatia.
4. Consome o cursor em *mini-lotes* (`batch_size`), acumulando em um `buffer`.
5. Sempre que o `buffer` atinge 10 000 docs (ou ao final da fatia) -> faz flush:
   - normaliza DataFrame -> prepara -> faz upload (parquet particionado).
   - conta documentos e libera memória.
6. Controle de erros:
   - 5 tentativas com back-off exponencial para `AutoReconnect`;
   - validações de parâmetros e tipos, logs detalhados.
"""


from __future__ import annotations

import gc
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.errors import AutoReconnect

from pipelines.datalake.utils.tasks import prepare_dataframe_for_upload
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import upload_df_to_datalake

# -------------------------------------------------------------- Tipos auxiliares
ValorSlice = Union[int, float, datetime, pd.Timestamp]


# -------------------------------------------------------------- Funções auxiliares
def _flush_e_enviar(
    buffer: List[Dict[str, Any]],
    flow_name: str,
    flow_owner: str,
    bq_dataset_id: str,
    bq_table_id: str,
) -> int:
    """
    Converte lista de documentos -> DataFrame -> parquet -> Data Lake.

    Retorna o número de documentos enviados.
    """
    n_docs = len(buffer)
    if n_docs == 0:  # proteção extra
        return 0

    df = pd.json_normalize(buffer)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)

    log(f"Preparando DataFrame ({n_docs} docs) para upload…")
    df_pronto = prepare_dataframe_for_upload.run(
        df=df,
        flow_name=flow_name,
        flow_owner=flow_owner
    )

    log("Enviando lote para o BigQuery…")
    upload_df_to_datalake.run(
        df=df_pronto,
        table_id=bq_table_id,
        dataset_id=bq_dataset_id,
        partition_column="data_extracao",
        source_format="parquet",
    )

    # limpeza de memória
    del df, df_pronto
    buffer.clear()
    gc.collect()

    log(f"Lote de {n_docs} documentos enviado com sucesso.")
    return n_docs


# -------------------------------------------------------------- Tarefa principal
@task(max_retries=5, retry_delay=timedelta(minutes=3))
def dump_collection_por_fatias(
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
    """
    Extrai documentos de `collection_name` paginando por `slice_var`
    e envia cada fatia ao Data Lake.

    Parâmetros
    ----------
    slice_var  : campo usado para fatiar (int/float ou datetime/timestamp)
    slice_size : tamanho da fatia (unidades numéricas ou dias)

    Retorna
    -------
    int
        Quantidade total de documentos enviados.
    """

    # -------------------------- validações iniciais ------------ #
    if slice_size <= 0:
        raise ValueError("`slice_size` deve ser > 0.")

    # Conexão MongoDB com timeouts
    conn_str = (
        f"mongodb://{user}:{password}@{host}:{port}/"
        f"?authSource={authsource}"
        "&connectTimeoutMS=20000"
        "&socketTimeoutMS=300000"
        "&serverSelectionTimeoutMS=20000"
        "&socketKeepAlive=true"
    )

    log("Conectando ao MongoDB…")
    with MongoClient(conn_str) as cliente_mongo:
        db = cliente_mongo[db_name]
        colecao = db[collection_name]
        log(f"Collection: {collection_name}")

        # ---------------------- n de docs para conferir posteriormente se obtemos todos ------- #
        filtro_base: Dict[str, Any] = query or {}
        total_esperado = colecao.count_documents(filtro_base)
        log(f"Total de documentos na collection com filtro = {total_esperado:,d} documentos")

        # ------------------ obtém valores min/max do slice ---------------- #
        log(f"Buscando menor e maior valor de `{slice_var}`…")

        menor_doc = next(
            colecao.find(filtro_base, {slice_var: 1, "_id": 0})
            .sort(slice_var, ASCENDING)
            .limit(1),
            None,
        )
        maior_doc = next(
            colecao.find(filtro_base, {slice_var: 1, "_id": 0})
            .sort(slice_var, DESCENDING)
            .limit(1),
            None,
        )

        if menor_doc is None or maior_doc is None:
            log("Nenhum documento encontrado com o filtro fornecido. Encerrando.")
            return 0

        menor_valor: ValorSlice = menor_doc[slice_var]
        maior_valor: ValorSlice = maior_doc[slice_var]

        if not isinstance(menor_valor, (int, float, datetime, pd.Timestamp)) or not isinstance(
            maior_valor, (int, float, datetime, pd.Timestamp)
        ):
            raise TypeError(f"`slice_var` deve ser numérico ou data; obtido {type(menor_valor)}")

        log(f"Intervalo detectado: {menor_valor} -> {maior_valor}")

        # ------------------------- loop por fatias ------------------------ #
        documentos_enviados = 0
        inicio_fatia: ValorSlice = menor_valor

        log(f"Iniciando extração em fatias de {slice_size}…")
        while inicio_fatia <= maior_valor:
            # define fim da fatia
            if isinstance(inicio_fatia, (int, float)):
                fim_fatia = inicio_fatia + slice_size
            else:  # datetime ou Timestamp
                fim_fatia = inicio_fatia + timedelta(days=slice_size)

            filtro_fatia = {
                **filtro_base,
                slice_var: {"$gte": inicio_fatia, "$lt": fim_fatia},
            }
            log(f"Fatia `{inicio_fatia}` -> `{fim_fatia}`")

            # ----------- tentativa com retry & back-off para AutoReconnect -------------------- #
            backoff_segundos = 5
            for tentativa in range(1, 6):
                try:
                    cursor = (
                        colecao.find(filtro_fatia, no_cursor_timeout=True)
                        .batch_size(1_000)
                        .max_time_ms(30_000)
                    )
                    break  # sucesso
                except AutoReconnect as erro:
                    log(
                        f"AutoReconnect (tentativa {tentativa}/5) "
                        f"-> aguardando {backoff_segundos}s: {erro}"
                    )
                    time.sleep(backoff_segundos)
                    backoff_segundos *= 2  # aumento exponencial do tempo de espera
            else:
                raise RuntimeError("Falha permanente após 5 tentativas de AutoReconnect.")

            # -------------------- itera cursor & faz flush ---------------- #
            buffer: List[Dict[str, Any]] = []
            try:
                for doc in cursor:
                    buffer.append(doc)
                    if len(buffer) >= 10_000:
                        documentos_enviados += _flush_e_enviar(
                            buffer,
                            flow_name,
                            flow_owner,
                            bq_dataset_id,
                            bq_table_id,
                        )
                # flush residual
                if buffer:
                    documentos_enviados += _flush_e_enviar(
                        buffer,
                        flow_name,
                        flow_owner,
                        bq_dataset_id,
                        bq_table_id,
                    )
            finally:
                cursor.close()  # sempre fecha cursor

            log(f"Fatia concluída. Total parcial = {documentos_enviados:,d} documentos.")
            inicio_fatia = fim_fatia  # avança para próxima fatia

    # -------------------------- pós-processamento ------------------------- #
    if documentos_enviados == total_esperado:
        log(f"Processo concluído! {documentos_enviados:,d} documentos enviados.")
    else:
        mensagem = (
            f"Erro de consistência: enviados {documentos_enviados:,d} "
            f"!= esperado {total_esperado:,d}."
        )
        log(f"{mensagem}")
        raise RuntimeError(mensagem)

    return documentos_enviados
