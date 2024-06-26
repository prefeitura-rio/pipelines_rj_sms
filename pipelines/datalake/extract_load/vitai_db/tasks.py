# -*- coding: utf-8 -*-
import os
from datetime import timedelta

import pandas as pd
import psycopg2
from sqlalchemy.exc import ProgrammingError

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task()
def get_table_names():
    return [
        "paciente",
        "alergia",
        "atendimento",
        "boletim",
        "cirurgia",
        "classificacao_risco",
        "diagnostico",
        "exame",
        "internacao",
        "profissional",
        "m_estabelecimento",
        "produto_saldo_atual",
    ]


@task(max_retries=3, retry_delay=timedelta(seconds=120))
def download_table_data_to_csv(
    db_url: str, table_name: str, start_datetime: str, end_datetime: str
) -> pd.DataFrame:
    try:
        df = pd.read_sql(
            f"""
            select *
            from basecentral.{table_name}
            where datahora between '{start_datetime}' and '{end_datetime}'
            """,
            db_url,
            dtype=str
        )
    except ProgrammingError as e:
        if isinstance(e.orig, psycopg2.errors.InsufficientPrivilege):
            log(
                f"Insufficient privilege to table basecentral.`{table_name}`. Ignoring table.",
                level="error",
            )
            return ""
    
    if 'id' in df.columns:
        log("Detected `id` column in dataframe. Renaming to `gid`", level="warning")
        df.rename(columns={'id':'gid'}, inplace=True)
    
    df['datalake__imported_at'] = pd.Timestamp.now(tz='America/Sao_Paulo')
    log(f"Added `imported_at` column to dataframe with current timestamp: {df['imported_at'].iloc[0]}") #noqa

    if not os.path.isdir("./tabledata"):
        log("Creating tabledata directory")
        os.mkdir("./tabledata")

    file_path = f"""./tabledata/{table_name}-{start_datetime}-{end_datetime}.csv"""  # noqa
    log(f"Saving table data to {file_path}")
    df.to_csv(file_path, index=False, header=True, sep=";")

    return file_path
