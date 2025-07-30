# -*- coding: utf-8 -*-
"""
Tarefas para Extração extração do MongoDB e carga para o BigQuery
==================================================================

Fluxo resumido
--------------
1. Descobre menor e maior valor de `slice_var`.
2. Gera faixas [(início, fim), …] de tamanho `slice_size`.
3. Para cada faixa:
   3.1 Consulta MongoDB em páginas (`BATCH_SIZE`).
   3.2 Acumula em `buffer`; a cada `FLUSH_THRESHOLD` docs -> faz flush.
4. Cada flush:
   - Normaliza DataFrame (helper `prepare_dataframe_for_upload`).
   - Sobe parquet particionado ao BigQuery (`upload_df_to_datalake`).
5. Ao final, valida se total enviado == total existente.
"""

from __future__ import annotations

import gc
from datetime import datetime, timedelta
from typing import Any, Dict, List, Sequence, Tuple, TypeAlias, Union

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log
from pymongo import ASCENDING, DESCENDING, MongoClient

from pipelines.datalake.utils.tasks import prepare_dataframe_for_upload
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import upload_df_to_datalake

# Configurações gerais
# -----------------------------------------------------------------------------#
BATCH_SIZE: int = 10_000  # tamanho do batch ao iterar no cursor
FLUSH_THRESHOLD: int = 10_000  # dispara upload quando buffer atinge este valor

ValorSlice: TypeAlias = Union[int, float, datetime, pd.Timestamp]


# Helpers
# -----------------------------------------------------------------------------#
def _build_conn_string(
    host: str,
    port: int,
    user: str,
    password: str,
    authsource: str,
) -> str:
    """Monta a connection-string com time-outs e keep-alive adequados."""
    return (
        f"mongodb://{user}:{password}@{host}:{port}/"
        f"?authSource={authsource}"
        "&connectTimeoutMS=20000"
        "&socketTimeoutMS=300000"
        "&serverSelectionTimeoutMS=20000"
        "&socketKeepAlive=true"
    )


def _get_extreme_value(collection, filtro: Dict[str, Any], campo: str, ordem: int) -> ValorSlice:
    """Retorna o menor ou maior valor de `campo` conforme `ordem`."""
    try:
        doc = collection.find(filtro, {campo: 1, "_id": 0}).sort(campo, ordem).limit(1).next()
        return doc[campo]
    except StopIteration:
        raise ValueError(
            f"Nenhum documento encontrado para o campo '{campo}' com o filtro fornecido."
        )


def _generate_slices(
    minimo: ValorSlice,
    maximo: ValorSlice,
    slice_size: int,
) -> List[Tuple[ValorSlice, ValorSlice]]:
    """Gera faixas de fatiamento de tamanho `slice_size`."""

    if slice_size <= 0:
        raise ValueError("O parâmetro `slice_size` deve ser maior que zero.")

    if minimo >= maximo:
        raise ValueError("O valor de `minimo` deve ser menor que `maximo`.")

    faixas: List[Tuple[ValorSlice, ValorSlice]] = []
    atual = minimo

    while atual < maximo:
        if isinstance(atual, (int, float)):
            fim = atual + slice_size
        elif isinstance(atual, (datetime, pd.Timestamp)):
            fim = atual + timedelta(days=slice_size)
        else:
            raise TypeError(f"Tipo não suportado para fatiamento: {type(atual)}")

        if fim > maximo:
            fim = maximo

        faixas.append((atual, fim))
        if fim == atual:  # segurança contra loop infinito
            break
        atual = fim

    if not faixas:
        raise ValueError("Nenhuma faixa de fatiamento gerada.")

    return faixas


def _flush_and_upload(
    buffer: List[Dict[str, Any]],
    flow_name: str,
    flow_owner: str,
    bq_dataset_id: str,
    bq_table_id: str,
) -> int:
    """
    Converte `buffer` -> DataFrame -> Parquet -> BigQuery.
    Só limpa o buffer após upload com sucesso.
    """
    if not buffer:
        return 0

    df = pd.json_normalize(buffer)

    # Deduplicação pelo _id (mantém idempotência)
    if "_id" in df.columns:
        df["_id"] = df["_id"].astype(str)
        df.drop_duplicates(subset="_id", keep="last", inplace=True)

    log(f"Preparando DataFrame ({len(df)} docs) para upload…")
    df_ready = prepare_dataframe_for_upload.run(
        df=df,
        flow_name=flow_name,
        flow_owner=flow_owner,
    )

    try:
        log("Enviando lote para o BigQuery…")
        upload_df_to_datalake.run(
            df=df_ready,
            table_id=bq_table_id,
            dataset_id=bq_dataset_id,
            partition_column="data_extracao",
            source_format="parquet",
        )
    except Exception as exc:
        log(f"Falha no upload: {exc}")
        raise  # Prefect fará retry mantendo o buffer intacto
    else:
        enviados = len(df)
        buffer.clear()
        gc.collect()
        log(f"Lote de {enviados} documentos enviado com sucesso.")
        return enviados


# Tasks Prefect
# -----------------------------------------------------------------------------#
@task(max_retries=5, retry_delay=timedelta(minutes=3), nout=2)
def gerar_faixas_de_fatiamento(
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
) -> Tuple[List[Tuple[ValorSlice, ValorSlice]], int]:
    """
    Conecta ao MongoDB e devolve as faixas [(início, fim), …] para fatiamento.

    O campo `slice_var` pode ser numérico ou datetime.
    """
    conn = _build_conn_string(host, port, user, password, authsource)
    with MongoClient(conn) as cli:
        col = cli[db_name][collection_name]
        filtro = query or {}

        log(f"Buscando extremos de `{slice_var}`…")
        minimo = _get_extreme_value(col, filtro, slice_var, ASCENDING)
        maximo = _get_extreme_value(col, filtro, slice_var, DESCENDING)

        log("Obtendo a quantidade de documentos na coleção…")
        total_esperado = col.count_documents(filtro)

        # Valida tipo
        if type(minimo) is not type(maximo):
            raise TypeError(
                "`slice_var` deve ter tipo único; " f"detectado {type(minimo)} e {type(maximo)}"
            )

        log(f"Intervalo detectado: {minimo} -> {maximo}")
        log(f"Gerando faixas de tamanho {slice_size}…")
        faixas = _generate_slices(minimo, maximo, slice_size)
        log(f"Total de faixas: {len(faixas)}")
        return faixas, total_esperado


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
    faixa: Tuple[ValorSlice, ValorSlice],
    bq_dataset_id: str,
    bq_table_id: str,
) -> int:
    """
    Extrai documentos da `faixa` e envia para o Data Lake.

    Retorna quantidade de documentos enviados.
    """
    conn = _build_conn_string(host, port, user, password, authsource)
    gte, lte = faixa
    filtro = {**(query or {}), slice_var: {"$gte": gte, "$lte": lte}}

    documentos_enviados = 0
    buffer: List[Dict[str, Any]] = []

    log(f"Iniciando extração para faixa {gte} -> {lte}…")
    with MongoClient(conn) as cli:
        col = cli[db_name][collection_name]
        cursor = (
            col.find(filtro, no_cursor_timeout=True).batch_size(BATCH_SIZE).max_time_ms(120_000)
        )

        try:
            for doc in cursor:
                buffer.append(doc)
                if len(buffer) >= FLUSH_THRESHOLD:
                    documentos_enviados += _flush_and_upload(
                        buffer, flow_name, flow_owner, bq_dataset_id, bq_table_id
                    )

            # Flush residual
            documentos_enviados += _flush_and_upload(
                buffer, flow_name, flow_owner, bq_dataset_id, bq_table_id
            )
        finally:
            cursor.close()

    log(f"Fatia concluída: {documentos_enviados:,d} docs enviados.")
    return documentos_enviados


@task
def validar_total_documentos(
    total_esperado: int,
    docs_por_fatia: Sequence[int],
    flow_name: str,
    flow_owner: str,
) -> None:
    """
    Compara total enviado vs. total existente na coleção.
    Dispara alerta no monitor se houver divergência acima de 3%.
    """

    total_enviado = sum(docs_por_fatia)
    log(f"Esperado: {total_esperado:,d} · Enviado: {total_enviado:,d}")

    if total_esperado == 0:
        log("Total esperado é zero, nada a validar.")
        return

    diff = abs(total_enviado - total_esperado)
    percent_diff = diff / total_esperado

    if percent_diff > 0.05:
        log(
            f"❌ Inconsistência detectada: "
            f"coleção tem {total_esperado:,d} docs, "
            f"mas apenas {total_enviado:,d} chegaram ao Data Lake "
            f"({percent_diff:.2%} de diferença)."
        )

        # Envia alerta para o monitor (Discord)
        msg = (
            f" <@{flow_owner}> Inconsistência: coleção tem {total_esperado:,d} docs, "
            f"mas apenas {total_enviado:,d} chegaram ao Data Lake "
            f"({percent_diff:.2%} de diferença)."
        )
        send_message(
            title=f"❌ Erro no Fluxo {flow_name}",
            message=msg,
            monitor_slug="error",
        )
        raise ValueError(msg)
