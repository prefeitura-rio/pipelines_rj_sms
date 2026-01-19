# -*- coding: utf-8 -*-
# flake8: noqa: E203
# pylint: disable= C0301
"""
General task functions for the data lake pipelines
"""

import os
from datetime import datetime, timedelta
from typing import Tuple

import pandas as pd
import prefect
from prefect.client import Client
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.utils.data_transformations import convert_str_to_date
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import upload_df_to_datalake


@task
def rename_current_flow_run(environment: str, is_routine: bool = True, **kwargs) -> None:
    """
    Renames the current flow run with a new name based on the provided parameters.

    Parameters:
    - environment (str): The environment for which the flow run is being renamed.
    - target_date (date): The target date for the flow run.
    - is_routine (bool, optional): Indicates whether the flow run is a routine or a reprocess. Defaults to False.
    - **kwargs: Additional keyword arguments that can be used to provide more information for the flow run name.

    Returns:
    None

    Example:
        rename_current_flow_run(environment='prod', target_date=date(2022, 1, 1), is_routine=True, param1='value1', param2='value2')
    """

    flow_run_id = prefect.context.get("flow_run_id")

    title = "Routine" if is_routine else "Reprocess"

    params = [f"{key}={value}" for key, value in kwargs.items() if key not in ["ap", "target_date"]]
    params.append(f"env={environment}")

    if "ap" in kwargs:
        ap = "all" if kwargs.get("ap") is None else kwargs.get("ap")
        params.append(f"ap={ap}")

    params = sorted(params)

    if "target_date" in kwargs:
        target_date = kwargs.get("target_date")
        target_date = convert_str_to_date(target_date)
    else:
        target_date = prefect.context.get("scheduled_start_time").date()

    flow_run_name = f"{title} ({', '.join(params)}): {target_date}"

    client = Client()
    client.set_flow_run_name(flow_run_id, flow_run_name)

    log(f"Flow run {flow_run_id} renamed to {flow_run_name}")


@task
def handle_columns_to_bq(df):
    log("Transformando colunas para adequação ao Big Query.")
    df.columns = remove_columns_accents(df)
    log("Colunas transformadas para adequação ao Big Query.")
    return df


@task
def prepare_dataframe_for_upload(df, flow_name, flow_owner):
    """
    Prepara o DataFrame para upload, realizando transformações e validações necessárias.
    Envia notificações em caso de erros.

    df: DataFrame contendo os dados que serão carregados para o Big Query.
    flow_name: Nome do fluxo.
    flow_owner: Usuário que deve ser alertado no Discord em caso de erros.
    """

    # Verifica se o DataFrame está vazio
    if df.empty:
        mensagem_erro = f" <@{flow_owner}> O DataFrame está vazio."
        send_message(
            title=f"❌ Erro no Fluxo {flow_name}",
            message=mensagem_erro,
            monitor_slug="error",
        )
        raise ValueError(mensagem_erro)

    # Tenta remover acentos dos nomes das colunas
    try:
        df.columns = remove_columns_accents(df)
        log("Acentos removidos dos nomes das colunas com sucesso.")
    except Exception as e:
        mensagem_erro = f" <@{flow_owner}> Erro ao remover acentos dos nomes das colunas: {str(e)}"
        send_message(
            title=f"❌ Erro no Fluxo {flow_name}",
            message=mensagem_erro,
            monitor_slug="error",
        )
        raise ValueError(mensagem_erro)

    # Tenta adicionar timestamp de extração (posteriormente utilizado para particionamento)
    try:
        df["data_extracao"] = datetime.now()
        log("Coluna 'data_extracao' adicionada com sucesso.")
    except Exception as e:
        mensagem_erro = f" <@{flow_owner}> Erro ao criar a coluna 'data_extracao': {str(e)}"
        send_message(
            title=f"❌ Erro no Fluxo {flow_name}",
            message=mensagem_erro,
            monitor_slug="error",
        )
        raise ValueError(mensagem_erro)

    return df


# Suíte para arquivos grandes demais para a memória:
# 1 -
@task
def gerar_faixas_de_data(
    data_inicial: str, data_final: str, dias_por_faixa: int = 1, date_format="%d/%m/%Y"
):
    """
    Gera uma lista de tuplas (inicio, fim) dividindo o intervalo
    entre data_inicial e data_final em blocos de tamanho 'dias_por_faixa'.

    Se data_final for a string "now", será convertido para o datetime atual.
    O parâmetro date_format define o formato das datas de entrada e saída.
    """

    # Função auxiliar para converter string para datetime usando o formato informado
    def parse_date(date_str, fmt):
        if isinstance(date_str, datetime):
            return date_str
        return datetime.strptime(date_str, fmt)

    # Converte data_inicial
    dt_inicial = parse_date(data_inicial, date_format)

    # Converte data_final
    if isinstance(data_final, datetime):
        dt_final = data_final
    elif isinstance(data_final, str) and data_final.lower() == "now":
        dt_final = datetime.now()
    else:
        dt_final = parse_date(data_final, date_format)

    log("Gerando faixas de datas para processamento em lotes.")
    faixas = []
    dt_atual = dt_inicial
    while dt_atual <= dt_final:
        dt_chunk_inicio = dt_atual
        dt_chunk_fim = dt_chunk_inicio + timedelta(days=dias_por_faixa - 1)
        if dt_chunk_fim > dt_final:
            dt_chunk_fim = dt_final
        faixa_inicio_str = dt_chunk_inicio.strftime(date_format)
        faixa_fim_str = dt_chunk_fim.strftime(date_format)
        faixas.append((faixa_inicio_str, faixa_fim_str))
        dt_atual = dt_chunk_fim + timedelta(days=1)

    log(f"{len(faixas)} Faixas de datas geradas com sucesso.")
    return faixas


# 2 -
@task
def extrair_inicio(faixa: Tuple[str, str]) -> str:
    """
    Extrai o início do intervalo da tupla.
    """
    return faixa[0]


# 3 -
@task
def extrair_fim(faixa: Tuple[str, str]) -> str:
    """
    Extrai o fim do intervalo da tupla.
    """
    return faixa[1]


# 4 -
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
        df=df, flow_name=flow_name, flow_owner=flow_owner
    )

    # Salva como outro arquivo Parquet
    prepared_path = file_path.replace(".parquet", "_prepared.parquet")
    df_prepared.to_parquet(prepared_path, index=False)

    return prepared_path


# 5 -
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
        source_format=source_format,
    )


# 6 -
@task
def delete_file(file_path: str):
    """
    Deleta o arquivo local para liberar espaço em disco.
    """
    # Comentário em português: removendo o arquivo do disco
    if os.path.exists(file_path):
        os.remove(file_path)
