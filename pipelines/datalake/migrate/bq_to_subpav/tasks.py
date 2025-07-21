# -*- coding: utf-8 -*-
"""
Tasks para migração de dados do BigQuery para o MySQL da SUBPAV.
"""

from datetime import timedelta
import pandas as pd
from typing import Optional, List
import re

from sqlalchemy.exc import SQLAlchemyError
from google.cloud import bigquery

from pipelines.utils.logger import log
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.datalake.migrate.bq_to_subpav.utils import get_mysql_engine


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def query_bq_table(
    dataset_id: str,
    table_id: str,
    project_id: str,
    environment: str,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Task de consulta aos dados no BigQuery.
    Args:
        dataset_id: Nome do dataset no BigQuery.
        table_id: Nome da tabela.
        project_id: ID do projeto GCP.
        environment: Ambiente de execução (ex: dev, prod).
        columns: Lista de colunas a selecionar (opcional).

    Returns:
        DataFrame com os dados retornados da consulta.
    """
    full_ref = f"{project_id}.{dataset_id}.{table_id}"
    select_clause = ", ".join(columns) if columns else "*"
    query = f"SELECT {select_clause} FROM `{full_ref}`"

    try:
        client = bigquery.Client(project=project_id)

        if columns:
            tbl = client.get_table(full_ref)
            available = {f.name for f in tbl.schema}
            missing_schema = set(columns) - available
            if missing_schema:
                raise ValueError(f"Colunas não existem no schema: {missing_schema}")

        df = client.query(query).to_dataframe()
        log(f"[{environment.upper()}] {full_ref} → {len(df)} registros", level="info")

        if columns:
            missing_df = set(columns) - set(df.columns)
            if missing_df:
                raise ValueError(f"Colunas ausentes no DataFrame: {missing_df}")

        return df

    except Exception as e:
        log(f"[{environment.upper()}] Erro ao consultar BigQuery ({full_ref}): {e}", level="error", force_discord_forwarding=True)
        raise


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def insert_df_into_mysql(
    df: pd.DataFrame,
    infisical_path: str,
    secret_name: str,
    db_schema: str,
    table_name: str,
    if_exists: str,
    environment: str = "dev",
    custom_insert_query: Optional[str] = None,
    batch_size: int = 50000,
):

    if df.empty:
        log(f"[{environment.upper()}] Nenhum dado para inserir em {table_name}.", level="info")
        return


    try:
        engine = get_mysql_engine(
            infisical_path=infisical_path,
            secret_name=secret_name,
            environment=environment,
        )
        log(f"[{environment.upper()}] Iniciando conexão MySQL", level="info")

        conn = engine.raw_connection()
        cursor = conn.cursor()
    except SQLAlchemyError as conn_err:
        log(f"[{environment.upper()}] Falha na conexão ao MySQL: {conn_err}", level="error")
        raise

    try:
        if custom_insert_query:
            log(f"[{environment.upper()}] Inserção custom em lote (batch_size={batch_size})", level="info")
            cols = set(re.findall(r"%\((\w+)\)s", custom_insert_query))
            missing = cols - set(df.columns)
            if missing:
                raise ValueError(f"Colunas faltando no DataFrame: {missing}")

            records = df.to_dict(orient="records")
            inserted, errors = 0, 0

            for i in range(0, len(records), batch_size):
                batch = records[i : i + batch_size]
                try:
                    cursor.executemany(custom_insert_query, batch)
                    conn.commit()
                    inserted += cursor.rowcount
                except Exception as e:
                    log(f"[{environment.upper()}] Erro no batch {i}-{i+batch_size}: {e}", level="error")
                    conn.rollback()
                    errors += len(batch)

            log(f"[{environment.upper()}] Custom insert concluído: inseridos={inserted}, erros={errors}", level="info")

        else:
            log(f"[{environment.upper()}] Inserção via pandas.to_sql (chunksize={batch_size})", level="info")
            df.to_sql(
                name=table_name,
                con=engine,
                schema=db_schema,
                if_exists=if_exists,
                index=False,
                chunksize=batch_size,
            )
            log(f"[{environment.upper()}] to_sql concluído: {len(df)} registros", level="info")

    finally:
        cursor.close()
        conn.close()
