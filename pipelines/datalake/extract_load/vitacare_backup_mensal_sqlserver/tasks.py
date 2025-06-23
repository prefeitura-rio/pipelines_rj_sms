# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""
from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytz
from google.cloud import bigquery
from prefect.engine.signals import SKIP
from sqlalchemy import create_engine, text

from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.utils import (
    transform_dataframe,
    clean_ut_id,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=2, retry_delay=timedelta(minutes=1))
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
        "ATENDIMENTOS": "acto_id",
        "PACIENTES": "cadastro",
    }
    bq_table_id = bq_table_rename.get(db_table.upper(), db_table.lower())

    try:
        full_table_name = f"{db_schema}.{db_table}"
        log(f"Iniciando processo para {full_table_name} do CNES: {cnes_code}")
        db_name = f"vitacare_historic_{cnes_code}"

        # Cria string de conexão e engine
        connection_string = (
            f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
        engine = create_engine(connection_string)

        query = f"SELECT * FROM {full_table_name}"
        total_rows = 0

        # Lógica para tabela ATENDIMENTOS (com chunking)
        if db_table.upper() == "ATENDIMENTOS":
            log(f"Extraindo tabela '{db_table}' em chunks para o CNES {cnes_code}.")
            chunks = pd.read_sql(query, engine, chunksize=100000)
            is_first_chunk = True
            
            for chunk in chunks:
                # Transforma o lote
                processed_chunk = transform_dataframe(chunk, cnes_code, db_table) 
                
                if processed_chunk.empty:
                    continue

                upload_df_to_datalake.run(
                    df=processed_chunk,
                    dataset_id=dataset_id,
                    table_id=bq_table_id,
                    partition_column=partition_column,
                    source_format="parquet",
                    if_exists="replace" if is_first_chunk else "append",
                    if_storage_data_exists="append",
                )
                is_first_chunk = False
                total_rows += len(processed_chunk) 

            # Se nenhum chunk foi processado (tabela vazia), loga e retorna skipped
            if total_rows == 0:
                 log(f"Nenhum dado retornado para a tabela '{db_table}' do CNES {cnes_code}. Pulando.")
                 return {"cnes": cnes_code, "status": "skipped", "reason": "No data extracted"}

            log(
                f"Carga do CNES {cnes_code} para a tabela '{bq_table_id}' concluída. Total de linhas: {total_rows}."
            )
            return {"cnes": cnes_code, "status": "success", "rows_loaded": total_rows}
        
        # Lógica para as demais tabelas (leitura completa)
        else:
            log(f"Extraindo tabela '{db_table}' para o CNES {cnes_code}.")
            df = pd.read_sql(query, engine)

            if df.empty:
                log(
                    f"Nenhum dado retornado para a tabela '{db_table}' do CNES {cnes_code}. Pulando."
                )
                return {"cnes": cnes_code, "status": "skipped", "reason": "No data extracted"}

            log(f"Extraídas {len(df)} linhas de {full_table_name} para o CNES {cnes_code}.")

            # Chama a função de transformação 
            processed_df = transform_dataframe(df, cnes_code, db_table) 

            log(f"Enviando {len(processed_df)} linhas do CNES {cnes_code} para o BigQuery.")
            upload_df_to_datalake.run(
                df=processed_df,
                dataset_id=dataset_id,
                table_id=bq_table_id,
                partition_column=partition_column,
                source_format="parquet",
                if_exists="append",
                if_storage_data_exists="append",
            )
            log(f"Carga do CNES {cnes_code} para a tabela '{bq_table_id}' concluída.")

            return {"cnes": cnes_code, "status": "success", "rows_loaded": len(processed_df)}

    except SKIP as e:
        log(f"Task pulada para CNES {cnes_code}: {e.message}", level="info")
        reason = "Table not found" if "Table not found" in e.message else "Database not found"
        return {"cnes": cnes_code, "status": "skipped", "reason": reason}

    except Exception as e:
        if "4060" in str(e) and "Cannot open database" in str(e):
            log(f"Database vitacare_historic_{cnes_code} não encontrado. Pulando.", level="warning")
            raise SKIP(f"Database for CNES {cnes_code} does not exist.")
        if "Invalid object name" in str(e):
            log(
                f"Tabela {full_table_name} não encontrada para o CNES {cnes_code}. Pulando.",
                level="warning",
            )
            raise SKIP(f"Table not found: '{db_table}'")

        log(f"Falha no processo para CNES {cnes_code}: {e}", level="error")
        return {"cnes": cnes_code, "status": "failed", "error": str(e)[:300]}


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
            log("Nenhum código CNES encontrado no BigQuery para Vitacare.", level="warning")
            return []
        log(f"Encontrados {len(cnes_list)} códigos CNES no BigQuery.")
        return cnes_list
    except Exception as e:
        log(f"Erro ao buscar códigos CNES do BigQuery: {e}", level="error")
        raise


@task
def get_tables_to_extract() -> list:
    return vitacare_constants.TABLES_TO_EXTRACT.value
