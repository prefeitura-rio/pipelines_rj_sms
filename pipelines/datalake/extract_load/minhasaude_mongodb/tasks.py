# -*- coding: utf-8 -*-
"""
Tarefas · Extração paginada de coleção MongoDB -> Carga para o Data Lake (BigQuery)

Objetivo
--------
Extrair documentos em lotes (“fatias”) sem sobrecarregar RAM nem manter cursores
abertos por tempo excessivo e
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
   - validações de parâmetros e tipos, logs detalhados.
"""


from __future__ import annotations

import gc
from datetime import datetime, timedelta
from typing import Any, Dict, List, Union, Tuple

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from pymongo import ASCENDING, DESCENDING, MongoClient

from pipelines.datalake.utils.tasks import prepare_dataframe_for_upload
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import upload_df_to_datalake
from pipelines.utils.monitor import send_message


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
    df_pronto = prepare_dataframe_for_upload.run(df=df, flow_name=flow_name, flow_owner=flow_owner)

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


# -------------------------------------------------------------- Tarefas
@task(max_retries=5, retry_delay=timedelta(minutes=3))
def obter_faixas_de_fatiamento(
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
) -> List[Tuple[ValorSlice, ValorSlice]]:
    """
    Obtém as faixas de fatiamento para extração de dados de uma coleção MongoDB, com base em um campo de corte (numérico ou data).
    Conecta-se ao MongoDB, determina o menor e maior valor do campo de fatiamento (`slice_var`) considerando um filtro opcional,
    e gera uma lista de tuplas representando intervalos [(início, fim), ...] de tamanho `slice_size` para serem usados em extrações paralelas ou paginadas.

    Retorna:
        List[Tuple[ValorSlice, ValorSlice]]
            - Lista de tuplas (início, fim) representando cada faixa de fatiamento.
    """

    conn = f"mongodb://{user}:{password}@{host}:{port}/?authSource={authsource}"
    with MongoClient(conn) as cliente_mongo:
        db = cliente_mongo[db_name]
        colecao = db[collection_name]
        log(f"Collection: {collection_name}")

        # ---------------------- n de docs para conferir posteriormente se obtemos todos ------- #
        filtro_base: Dict[str, Any] = query or {}

        # ------------------ obtém valores min/max do slice ---------------- #
        log(f"Buscando menor e maior valor de `{slice_var}`…")

        menor_doc = next(
            colecao.find(filtro_base, {slice_var: 1, "_id": 0}).sort(slice_var, ASCENDING).limit(1),
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
            raise ValueError("Nenhum documento encontrado com o filtro fornecido.")

        menor_valor: ValorSlice = menor_doc[slice_var]
        maior_valor: ValorSlice = maior_doc[slice_var]

        if not isinstance(menor_valor, (int, float, datetime, pd.Timestamp)) or not isinstance(
            maior_valor, (int, float, datetime, pd.Timestamp)
        ):
            raise TypeError(f"`slice_var` deve ser numérico ou data; obtido {type(menor_valor)}")

        log(f"Intervalo detectado: {menor_valor} -> {maior_valor}")

        # ------------------------- gera faixas de fatiamento ------------------------ #
        log(f"Gerando faixas de fatiamento de tamanho {slice_size}…")
        faixas: List[Tuple[ValorSlice, ValorSlice]] = []
        atual: ValorSlice = menor_valor

        try:
            while atual <= maior_valor:
                # Define o fim da fatia conforme o tipo do valor (numérico ou data)
                if isinstance(atual, (int, float)):
                    fim = atual + slice_size
                elif isinstance(atual, (datetime, pd.Timestamp)):
                    fim = atual + timedelta(days=slice_size)
                else:
                    log(f"Tipo inesperado para fatiamento: {type(atual)}")
                    raise TypeError(f"Tipo não suportado para fatiamento: {type(atual)}")

                # Adiciona a faixa à lista
                faixas.append((atual, fim))
                log(f"Faixa adicionada: início={atual}, fim={fim}")

                # Previne loops infinitos em caso de dados inconsistentes
                if fim == atual:
                    log(
                        "Valor de fim igual ao início, possível loop infinito. Encerrando geração de faixas."
                    )
                    break

                atual = fim

                if not faixas:
                    log(
                        "Nenhuma faixa de fatiamento foi gerada. Verifique os parâmetros de entrada."
                    )
                    raise ValueError("Nenhuma faixa de fatiamento gerada.")

        except Exception as e:
            log(f"Erro ao gerar faixas de fatiamento: {e}")
            raise

        log(f"Total de faixas geradas: {len(faixas)}")
        return faixas


@task(max_retries=5, retry_delay=timedelta(minutes=3))
def extrair_fatia_para_datalake(
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
    faixa: Tuple[ValorSlice, ValorSlice],
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

    gte, lt = faixa
    filtro = {**(query or {}), slice_var: {"$gte": gte, "$lt": lt}}

    log("Conectando ao MongoDB…")
    documentos_enviados = 0
    with MongoClient(conn_str) as cliente_mongo:
        db = cliente_mongo[db_name]
        colecao = db[collection_name]
        log(f"Collection: {collection_name}")

        cursor = (
            colecao.find(filtro, no_cursor_timeout=True).batch_size(10_000).max_time_ms(120_000)
        )

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

    return documentos_enviados


@task
def validar_total_documentos(
    host: str,
    port: int,
    user: str,
    password: str,
    authsource: str,
    db_name: str,
    collection_name: str,
    query: dict | None,
    docs_por_fatia: List[int],
    flow_name: str,
    flow_owner: str,
) -> None:
    """
    Conta os documentos na coleção **uma vez**, soma o que já foi
    enviado por todas as fatias e dispara erro se houver diferença.
    """
    conn = f"mongodb://{user}:{password}@{host}:{port}/?authSource={authsource}"
    with MongoClient(conn) as cli:
        col = cli[db_name][collection_name]
        total_esperado = col.count_documents(query or {})

    total_enviado = sum(docs_por_fatia)
    log(f"Esperado: {total_esperado:,d} · Enviado: {total_enviado:,d}")

    if total_enviado != total_esperado:
        mensagem_erro = (
            f" @{flow_owner} Inconsistência - coleção tem {total_esperado:,d} documentos, "
            f"mas só {total_enviado:,d} foram enviados."
        )
        send_message(
            title=f"❌ Erro no Fluxo {flow_name}",
            message=mensagem_erro,
            monitor_slug="error",
        )
        raise ValueError(mensagem_erro)
