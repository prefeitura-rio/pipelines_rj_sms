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
from sqlalchemy import create_engine

from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    vitacare_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log

@task(max_retries=3, retry_delay=timedelta(seconds=30))
def extract_and_transform_table(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_schema: str,
    db_table: str,
    cnes_code: str,
) -> pd.DataFrame:
    """
    Função Python para extrair e transformar dados.
    """
    try:

        full_table_name = f"{db_schema}.{db_table}"
        log(f"Attempting to download data from {full_table_name} for CNES: {cnes_code}")
        db_name = f"vitacare_historic_{cnes_code}"

        connection_string = (
            f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
        engine = create_engine(connection_string)
        df = pd.read_sql(f"SELECT * FROM {full_table_name}", engine)
        log(f"Successfully downloaded {len(df)} rows from {full_table_name} for CNES {cnes_code}.")

        # Adiciona metadados da extração
        now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
        df["extracted_at"] = now
        df["id_cnes"] = cnes_code
        
        # Remove acentos dos nomes das colunas
        df.columns = remove_columns_accents(df)

        # Tratamento específico para a coluna `ut_id``
        tables_with_ut_id = ["PACIENTES", "ATENDIMENTOS"]
        if db_table.upper() in tables_with_ut_id and "ut_id" in df.columns:
            
            def clean_ut_id(val):
                if isinstance(val, bytes):
                    try:
                        # Tenta decodificar com o encoding mais provável, removendo nulos
                        return val.decode("utf-16-le", errors="ignore").replace("\x00", "").strip()
                    except UnicodeDecodeError:
                        # Fallback para outro encoding comum
                        return val.decode("latin-1", errors="ignore").replace("\x00", "").strip()
                # Garante que qualquer outra coisa seja convertida para string
                return str(val).replace("\x00", "").strip()

            df["ut_id"] = df["ut_id"].apply(clean_ut_id)
            log(f"'ut_id' in {full_table_name} (CNES {cnes_code}) cleaned.")

        # Converte todas as colunas para string
        df = df.astype(str)
        log(f"All columns converted to string type for {full_table_name} (CNES {cnes_code}).")

        return df
    except Exception as e:
        if "4060" in str(e) and "Cannot open database" in str(e):
            log(
                f"Database vitacare_historic_{cnes_code} not found. Skipping task.",
                level="warning",
            )
            raise SKIP(f"Database for CNES {cnes_code} does not exist.")

        if "Invalid object name" in str(e):
            log(
                f"Table {full_table_name} not found for CNES {cnes_code}. Skipping task.",
                level="warning",
            )
            raise SKIP(f"Table not found: '{db_table}'")
        log(
            f"Error downloading or transforming data from {full_table_name} "
            f"(CNES: {cnes_code}): {e}",
            level="error",
        )
        raise

@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_vitacare_cnes_from_bigquery() -> list:
    """
    Busca a lista de códigos CNES distintos da tabela de estabelecimentos
    no BigQuery para prontuários Vitacare
    """
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

        # Coleta os resultados
        cnes_list = [str(row.id_cnes) for row in query_job if row.id_cnes is not None]

        if not cnes_list:
            log("Nenhum código CNES encontrado no BigQuery para Vitacare.", level="warning")
            # raise FAIL("Nenhum CNES encontrado para Vitacare no BigQuery")
            return []

        log(f"Encontrados {len(cnes_list)} códigos CNES no BigQuery.")
        return cnes_list
    except Exception as e:
        log(f"Erro ao buscar códigos CNES do BigQuery: {e}", level="error")
        raise


@task
def get_tables_to_extract() -> list:
    """Retorna a lista de tabelas a serem extraídas para um CNES"""
    return vitacare_constants.TABLES_TO_EXTRACT.value

@task
def build_bq_table_name(table_name: str) -> str:
    """Constrói o nome da tabela no BQ"""
    return table_name.lower()
