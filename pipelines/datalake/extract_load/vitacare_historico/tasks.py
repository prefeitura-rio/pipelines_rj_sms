# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""

import time
from datetime import timedelta

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine

from pipelines.datalake.extract_load.vitacare_historico.constants import (
    vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_historico.utils import (
    get_instance_status,
    set_instance_activation_policy,
    transform_dataframe,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(minutes=2))
def process_cnes_table(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_schema: str,
    db_table: str,
    cnes_code: str,
    dataset_id: str,
    partition_column: str,
):

    bq_table_rename = {
        "ATENDIMENTOS": "acto",
        "PACIENTES": "cadastro",
    }
    bq_table_id = bq_table_rename.get(db_table.upper(), db_table.lower())

    full_table_name = f"{db_schema}.{db_table}"
    log(f"[process_cnes_table] Iniciando extração para tabela {db_table} do CNES: {cnes_code}")

    db_name = f"vitacare_historic_{cnes_code}"

    connection_string = (
        f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    )

    try:
        engine = create_engine(connection_string)

        query = f"SELECT * FROM {full_table_name}"

        total_rows = 0
        is_first_chunk = True

        chunks = pd.read_sql(query, engine, chunksize=500000)
        data_processed_successfully = False

        for chunk in chunks:
            dataframe = transform_dataframe(chunk, cnes_code, db_table)

            if dataframe.empty:
                log(
                    f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} vazia. Upload ignorado",  # noqa
                    level="warning",
                )
                continue

            upload_df_to_datalake.run(
                df=dataframe,
                dataset_id=dataset_id,
                table_id=bq_table_id,
                partition_column=partition_column,
                source_format="parquet",
                if_exists="replace" if is_first_chunk else "append",
                if_storage_data_exists="replace" if is_first_chunk else "append",
            )

            is_first_chunk = False
            total_rows += len(dataframe)
            data_processed_successfully = True

            log(
                f"[process_cnes_table] Upload feito para o CNES {cnes_code} "
                f"Total de linhas: {total_rows}"
            )

        if not data_processed_successfully and total_rows == 0:
            log(
                f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} está vazia",
                level="warning",
            )

        else:
            log(
                f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} processada com sucesso.",  # noqa
                level="info",
            )
    except Exception as e:
        error_message = str(e)

        if "Invalid object name" in error_message:
            log(
                f"[process_cnes_table] Tabela {db_table} não encontrada para o CNES {cnes_code}",
                level="warning",
            )
        elif "Cannot open database" in error_message or "(4060)" in error_message:
            log(
                f"[process_cnes_table] Banco de dados {db_name} não encontrado para o CNES {cnes_code}",  # noqa
                level="warning",
            )
        else:
            log(f"[process_cnes_table] Erro inesperado {error_message[:250]}", level="error")
            raise


@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_vitacare_cnes_from_bigquery() -> list:
    query = """
        SELECT DISTINCT id_cnes
        FROM `rj-sms.saude_dados_mestres.estabelecimento`
        WHERE prontuario_versao = 'vitacare'
        AND prontuario_episodio_tem_dado = 'sim'
    """
    log(f"Buscando códigos CNES do BigQuery com a query: {query}")
    try:
        client = bigquery.Client()

        query_job = client.query(query)

        cnes_list = [str(row.id_cnes) for row in query_job if row.id_cnes is not None]

        if not cnes_list:
            log(
                "[get_vitacare_cnes_from_bigquery] Nenhum código CNES encontrado no BigQuery",
                level="warning",
            )
            return []
        log(f"[get_vitacare_cnes_from_bigquery] Encontrados {len(cnes_list)} unidades no BigQuery.")
        return cnes_list

    except Exception as e:
        log(
            f"[get_vitacare_cnes_from_bigquery] Erro ao buscar códigos CNES do BigQuery: {e}",
            level="error",
        )
        raise


@task
def get_tables_to_extract() -> list:
    return vitacare_constants.TABLES_TO_EXTRACT.value


@task
def build_operator_params(tables: list, env: str, schema: str, part_col: str) -> list:
    params_list = []
    for table in tables:
        params_list.append(
            {
                "table_name": table,
                "environment": env,
                "db_schema": schema,
                "partition_column": part_col,
                "rename_flow": True,
            }
        )
    return params_list


@task
def build_dbt_paramns(env: str):

    return {
        "command": "build",
        "environment": env,
        "exclude": None,
        "flag": None,
        "rename_flow": True,
        "select": "tag:vitacare_historico",
        "send_discord_report": False,
    }


@task(max_retries=2, retry_delay=timedelta(seconds=30))
def start_cloudsql_instance():
    status = get_instance_status()
    log("Iniciando a instância do Cloud SQL...")
    if status["state"] == "RUNNABLE" and status["activationPolicy"] == "ALWAYS":
        log("Instância já está no estado RUNNABLE.", level="info")
        return
    set_instance_activation_policy("ALWAYS")


@task
def wait_for_instance_runnable(max_wait_minutes: int = 10):
    log("Aguardando a instância ficar no estado RUNNABLE...")
    start_time = time.time()
    timeout = max_wait_minutes * 60

    while time.time() - start_time < timeout:
        status = get_instance_status()
        log(f"Status atual da instância: {status}")
        if status["state"] == "RUNNABLE" and status["activationPolicy"] == "ALWAYS":

            log("Instância ativada", level="info")
            return
        time.sleep(30)

    raise TimeoutError(f"A instância não atingiu o estado RUNNABLE em {max_wait_minutes} minutos.")


@task(max_retries=2, retry_delay=timedelta(seconds=30))
def stop_cloudsql_instance():
    log("Desligando a instância do Cloud SQL...")
    set_instance_activation_policy("NEVER")
