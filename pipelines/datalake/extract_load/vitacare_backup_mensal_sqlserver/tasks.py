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
    """
    Extrai, transforma e carrega dados de uma tabela para um único CNES.
    """
    bq_table_id = db_table.lower()
    try:
        # --- 1. Extração e Transformação ---
        full_table_name = f"{db_schema}.{db_table}"
        log(f"Iniciando processo para {full_table_name} do CNES: {cnes_code}")
        db_name = f"vitacare_historic_{cnes_code}"

        connection_string = (
            f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
        engine = create_engine(connection_string)

        query = ""
        if db_table.upper() == 'PRESCRICOES':
            # Query que corrige o backslash na coluna posologia.
            query = """
            SELECT
                acto_id,
                nome_medicamento,
                cod_medicamento,
                REPLACE(posologia, '\\', '/') AS posologia,
                quantidade,
                uso_continuado
            FROM dbo.prescricoes
            """
        else:
            # Para todas as outras tabelas, continua como antes.
            query = f"SELECT * FROM {full_table_name}"

        df = pd.read_sql(query, engine)


        if df.empty:
            log(f"Nenhum dado retornado para a tabela '{db_table}' do CNES {cnes_code}. Pulando.")
            return {"cnes": cnes_code, "status": "skipped", "reason": "No data extracted"}

        log(f"Extraídas {len(df)} linhas de {full_table_name} para o CNES {cnes_code}.")

        now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
        df["extracted_at"] = now
        df["id_cnes"] = cnes_code

        df.columns = remove_columns_accents(df)

        tables_with_ut_id = ["PACIENTES", "ATENDIMENTOS"]
        if db_table.upper() in tables_with_ut_id and "ut_id" in df.columns:

            def clean_ut_id(val):
                if isinstance(val, bytes):
                    try:
                        return val.decode("utf-16-le", errors="ignore").replace("\x00", "").strip()
                    except UnicodeDecodeError:
                        return val.decode("latin-1", errors="ignore").replace("\x00", "").strip()
                return str(val).replace("\x00", "").strip()

            df["ut_id"] = df["ut_id"].apply(clean_ut_id)

        df = df.astype(str)

        # --- 2. Carga ---
        log(f"Enviando {len(df)} linhas do CNES {cnes_code} para o BigQuery.")
        upload_df_to_datalake.run(
            df=df,
            dataset_id=dataset_id,
            table_id=bq_table_id,
            partition_column=partition_column,
            source_format="parquet",
            if_exists="append",
            if_storage_data_exists="append",
        )
        log(f"Carga do CNES {cnes_code} para a tabela '{bq_table_id}' concluída.")

        return {"cnes": cnes_code, "status": "success", "rows_loaded": len(df)}

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
    """Retorna a lista de tabelas a serem extraídas para um CNES"""
    return vitacare_constants.TABLES_TO_EXTRACT.value
