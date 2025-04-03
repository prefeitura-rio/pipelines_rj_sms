# -*- coding: utf-8 -*-
# flake8: noqa: E203
# pylint: disable= C0301
"""
General task functions for the data lake pipelines
"""
from datetime import datetime

import prefect
from prefect.client import Client
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.utils.data_transformations import convert_str_to_date
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.monitor import send_message


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
        mensagem_erro = f" @{flow_owner}. O DataFrame está vazio."
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
        mensagem_erro = f" @{flow_owner}. Erro ao remover acentos dos nomes das colunas: {str(e)}"
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
        mensagem_erro = f" @{flow_owner}. Erro ao criar a coluna 'data_extracao': {str(e)}"
        send_message(
            title=f"❌ Erro no Fluxo {flow_name}",
            message=mensagem_erro,
            monitor_slug="error",
        )
        raise ValueError(mensagem_erro)

    return df