# -*- coding: utf-8 -*-
"""
Tasks for SUBPAV Dump
"""

from datetime import datetime, timedelta
from math import ceil

import pandas as pd
import pytz

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def create_extraction_batches(
    db_url: str,
    db_schema: str,
    db_table: str,
    datetime_column: str,
    id_column: str,
    batch_size: int = 50000,
    date_filter: datetime = None,
) -> list[str]:
    """
    Cria uma lista de queries SQL para extrair dados de uma tabela em lotes (batches).

    Esta função calcula o número total de registros na tabela e gera múltiplas
    queries SQL para extrair os dados em partes, com base no tamanho definido
    para cada lote. Se um filtro de data for fornecido, ele será aplicado à extração.

    Args:
        db_url (str): URL de conexão com o banco de dados.
        db_schema (str): Nome do schema no banco de dados.
        db_table (str): Nome da tabela a ser extraída.
        datetime_column (str): Nome da coluna usada para filtrar por data.
        id_column (str): Coluna usada para ordenação e paginação.
        batch_size (int, optional): Número de linhas por lote. Default é 50.000.
        date_filter (datetime, optional): Data mínima para extração de registros.

    Returns:
        list[str]: Lista de queries SQL para extrair os dados em lotes.
    """
    sql_filter = ""
    if date_filter:
        sql_filter = f"WHERE {datetime_column} >= '{date_filter.strftime('%Y-%m-%d')}'"

    total_rows = pd.read_sql(
        f"SELECT COUNT(*) as quant FROM {db_schema}.{db_table} {sql_filter}", db_url
    ).iloc[0]["quant"]
    log(f"Total rows to download: {total_rows}")

    if total_rows <= batch_size:
        return [f"SELECT * FROM {db_schema}.{db_table} {sql_filter}"]

    num_batches = ceil(total_rows / batch_size)
    log(f"Number of batches to download: {num_batches}")

    queries = []
    for i in range(num_batches):
        query = f"""
            SELECT * FROM {db_schema}.{db_table} {sql_filter}
            ORDER BY {id_column} ASC
            LIMIT {batch_size} OFFSET {i * batch_size}
        """
        log(f"Query {i+1}: {query}")
        queries.append(query)

    return queries


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def download_from_db(
    db_url: str,
    query: str,
) -> pd.DataFrame:
    """
    Executa uma query SQL em um banco de dados e retorna os dados como um DataFrame.

    A função também adiciona a coluna `datalake_loaded_at` com a data/hora
    da extração e remove acentos dos nomes das colunas.

    Args:
        db_url (str): URL de conexão com o banco de dados.
        query (str): Comando SQL a ser executado.

    Returns:
        pd.DataFrame: DataFrame com os dados extraídos e coluna de timestamp incluída.
    """

    table = pd.read_sql(query, db_url)
    log(f"Downloaded {len(table)} rows from Table")

    table["datalake_loaded_at"] = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    table.columns = remove_columns_accents(table)
    return table


@task
def build_bq_table_name(db_table: str, schema: str) -> str:
    """
    Constrói o nome da tabela no padrão do BigQuery a partir do schema e nome da tabela original.

    Args:
        db_table (str): Nome da tabela no banco de dados original.
        schema (str): Nome do schema no banco de dados original.

    Returns:
        str: Nome da tabela no padrão BigQuery (schema__tabela).
    """
    return f"{schema}__{db_table}"
