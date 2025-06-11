# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""
from datetime import datetime, timedelta

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


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def extract_and_transform_table(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_schema: str,
    db_table: str,
    cnes_code: str,
) -> pd.DataFrame:
    full_table_name = f"{db_schema}.{db_table}"
    log(f"Attempting to download data from {full_table_name} for CNES: {cnes_code}")

    db_name = f"vitacare_historic_{cnes_code}"
    try:
        connection_string = (
            f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
        )
        engine = create_engine(connection_string)

        df = pd.read_sql(f"SELECT * FROM {full_table_name}", engine)
        log(f"Successfully downloaded {len(df)} rows from {full_table_name} for CNES {cnes_code}.")

        now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
        df["extracted_at"] = now
        df["id_cnes"] = cnes_code

        df.columns = remove_columns_accents(df)  #
        log(f"Transformed DataFrame for {full_table_name} from CNES {cnes_code}.")

        tables_with_ut_id = ["PACIENTES", "ATENDIMENTOS"]
        if db_table.upper() in tables_with_ut_id and "_ut_id" in df.columns:

            def clean_ut_id(val):
                if isinstance(val, bytes):
                    try:
                        decoded_val = val.decode("utf-16-le", errors="ignore")
                    except UnicodeDecodeError:
                        decoded_val = val.decode("latin-1", errors="ignore")
                    clean_str = (
                        decoded_val.replace("\x00", "")
                        .replace("\n", " ")
                        .replace("\r", " ")
                        .replace("\t", " ")
                    )
                    return (
                        val.hex()
                        if any(ord(c) < 32 and c not in (" ",) for c in clean_str)
                        else clean_str.strip()
                    )
                elif isinstance(val, str):
                    if val.startswith("b'") and val.endswith("'"):
                        byte_repr = (
                            val[2:-1].encode("latin-1").decode("unicode_escape").encode("latin-1")
                        )
                        try:
                            decoded_val = byte_repr.decode("utf-16-le", errors="ignore")
                        except UnicodeDecodeError:
                            decoded_val = byte_repr.decode("latin-1", errors="ignore")
                        clean_str = (
                            decoded_val.replace("\x00", "")
                            .replace("\n", " ")
                            .replace("\r", " ")
                            .replace("\t", " ")
                        )
                        return (
                            byte_repr.hex()
                            if any(ord(c) < 32 and c not in (" ",) for c in clean_str)
                            else clean_str.strip()
                        )
                    return (
                        val.replace("\x00", "")
                        .replace("\n", " ")
                        .replace("\r", " ")
                        .replace("\t", " ")
                        .strip()
                    )
                return (
                    str(val)
                    .replace("\x00", "")
                    .replace("\n", " ")
                    .replace("\r", " ")
                    .replace("\t", " ")
                    .strip()
                )

            df["_ut_id"] = df["_ut_id"].apply(clean_ut_id)
            log(f"'_ut_id' in {full_table_name} (CNES {cnes_code}) cleaned.")

        for col in df.select_dtypes(include=["object", "string"]).columns:
            current_col_data = df[col].astype(str)
            current_col_data = current_col_data.str.replace(r"[\n\r\t\x00]+", " ", regex=True)
            current_col_data = current_col_data.str.replace(
                '"', '""', regex=False
            )  # Escape double quotes for CSV
            df[col] = '"' + current_col_data + '"'
        log(f"All string columns for {full_table_name} (CNES {cnes_code}) prepared for CSV.")

        df = df.astype(str)
        log(f"Converted all columns to string type for {full_table_name} (CNES {cnes_code}).")

        for col_name in df.columns:
            if col_name.lower() == "acto_id":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                df[col_name] = df[col_name].astype(pd.Int64Dtype())
                log(
                    f"Column '{col_name}' in {full_table_name} (CNES {cnes_code}) converted to nullable integer."
                )
        return df
    except Exception as e:
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


@task
def build_bq_table_name(table_name: str) -> str:
    """Constrói nome da tabela no BigQuery"""
    bq_table_name = f"{table_name.lower()}"
    log(f"Built BigQuery table name: {bq_table_name} for source table: {table_name}")
    return bq_table_name
