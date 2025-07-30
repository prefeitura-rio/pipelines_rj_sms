# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""

from datetime import timedelta

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine

from pipelines.datalake.extract_load.vitacare_historico.constants import (
    vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_historico.utils import transform_dataframe
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
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
) -> dict:

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
                    f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} vazia. Upload ignorado",
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
            return {"status": "tabela_vazia", "cnes": cnes_code, "table": db_table}

        else:
            log(
                f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} processada com sucesso.",
                level="info",
            )
    except Exception as e:
        error_message = str(e)
        log(
            f"[process_cnes_table] Erro ao processar tabela {db_table} do CNES {cnes_code}",
            level="warning",
        )

        if "Invalid object name" in error_message:
            status = "nao_encontrado"
        else:
            status = "erro_inesperado"
            log(
                f"[process_cnes_table] Erro inesperado",
                level="error"    
            )

        return {
            "status": status,
            "cnes": cnes_code,
            "table": db_table,
        }


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


@task
def send_table_report(process_results_for_one_table: list[dict | None], table_name: str):

    tabelas_vazias = {}
    nao_encontrado = {}
    erro_inesperado = {}

    for result in process_results_for_one_table:
        if result is None:
            continue

        status = result.get("status")
        cnes = result.get("cnes")

        if status == "tabela_vazia":
            tabelas_vazias.setdefault(cnes, []).append(table_name)
        elif status == "nao_encontrado":
            nao_encontrado.setdefault(cnes, []).append(table_name)
        elif status == "erro_inesperado":
            erro_inesperado.setdefault(cnes, []).append(table_name)

    title = ""
    message = ""

    has_issues = bool(tabelas_vazias or nao_encontrado or erro_inesperado)

    if has_issues:
        title = f"🟡 Extração de Dados - Tabela `{table_name}` com Alertas"  #
        message_lines = [
            f"A extração de dados da tabela `{table_name}` foi concluída. Foram identificados os seguintes problemas:",
            "",
        ]

        # Lista de problemas no formato "CNES: [cnes], Tabela: [tabela], Status: [erro]"
        if nao_encontrado:
            for cnes in nao_encontrado.keys():  # Itera pelos CNES que deram erro
                message_lines.append(
                    f"- CNES: `{cnes}`, Status: Tabela ou banco de dados não encontrado"
                )

        if tabelas_vazias:
            for cnes in tabelas_vazias.keys():
                message_lines.append(f"- CNES: `{cnes}`, Status: Tabela vazia")

        if erro_inesperado:
            for cnes in erro_inesperado.keys():
                message_lines.append(f"- CNES: `{cnes}`, Status: Erro inesperado")

        message = "\n".join(message_lines)

    else:
        title = f"🟢 Extração de Dados - Tabela `{table_name}` com Sucesso"
        message = (
            f"A extração de dados da tabela `{table_name}` rodou com sucesso para todas as unidades"
        )

    send_message(
        title=title,
        message=message,
        monitor_slug="data-ingestion",
    )

    log(f"[send_table_report] Relatório da tabela '{table_name}' enviado para o Discord.")
