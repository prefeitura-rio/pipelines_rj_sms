# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""
from datetime import datetime, timedelta
import pandas as pd
import pytz
from sqlalchemy import create_engine
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log

@task
def get_sql_server_engine(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_name: str,
) -> str:
    """
    Cria e retorna uma URL de conexão para o SQL Server.
    """
    log(f"Building SQL Server engine for database: {db_name}")
    # O driver ODBC para SQL Server geralmente é 'ODBC Driver 17 for SQL Server' ou similar
    # Certifique-se de que o driver correto está instalado no ambiente de execução.
    connection_string = (
        f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?"
        "driver=ODBC Driver 17 for SQL Server"
    )
    # create_engine é do SQLAlchemy, que será usado para a conexão
    engine = create_engine(connection_string)
    log("SQL Server engine created successfully.")
    return engine

@task(max_retries=3, retry_delay=timedelta(seconds=90))
def extract_and_transform_table(
    db_url: str, # Recebe o engine de conexão criado por get_sql_server_engine
    db_schema: str,
    db_table: str,
    cnes_code: str,
) -> pd.DataFrame:
    """
    Extrai dados de uma tabela específica, adiciona colunas de metadados e faz a limpeza.
    """
    full_table_name = f"{db_schema}.{db_table}"
    log(f"Attempting to download data from {full_table_name} for CNES: {cnes_code}")

    try:
        # Extrai os dados completos da tabela. Para full load, isso é ideal.
        # Se as tabelas forem muito grandes, precisaremos de paginação aqui,
        # como no seu código anterior. Por enquanto, focando no básico.
        df = pd.read_sql(f"SELECT * FROM {full_table_name}", db_url)
        log(f"Successfully downloaded {len(df)} rows from {full_table_name}.")

        # Adiciona colunas de metadados
        now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        df["extracted_at"] = now
        df["id_cnes"] = cnes_code

        # Limpa nomes das colunas
        df.columns = remove_columns_accents(df)
        log(f"Transformed DataFrame for {full_table_name} from CNES {cnes_code}.")
        return df

    except Exception as e:
        log(f"Error downloading or transforming data from {full_table_name} (CNES: {cnes_code}): {e}", level="error")
        raise # Re-raise a exceção para que o Prefect possa lidar com as retries

@task
def build_bq_table_name(table_name: str) -> str:
    """
    Retorna o nome da tabela padronizado para o BigQuery.
    Ex: 'VACINAS' -> 'vacinas_historic'
    """
    # Garante que o nome da tabela esteja em minúsculas para o BigQuery
    bq_table_name = f"{table_name.lower()}_historic"
    log(f"Built BigQuery table name: {bq_table_name} for source table: {table_name}")
    return bq_table_name