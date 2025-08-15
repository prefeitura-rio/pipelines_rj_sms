# -*- coding: utf-8 -*-
"""
Tasks
"""
import io
import json

# Geral
import re
from datetime import datetime, timedelta
from typing import Literal

import pandas as pd
import requests

# Internos
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.ser_metabase.constants import SLICE_COLUMNS
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def authenticate_in_metabase(user: str, password: str) -> str:
    """
    Faz a autenticação e retorna o token que deve ser passado para as consultas
    """
    log("Iniciando autenticação no Metabase. Usuário: {}".format(user))
    auth_url = "https://metabase.saude.rj.gov.br/api/session"
    auth_payload = {"username": user, "password": password}
    auth_response = requests.post(auth_url, json=auth_payload, verify=False)

    token = auth_response.json()["id"]
    log("Autenticação concluída com sucesso.")
    return token


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def query_slice_limit(
    token: str, database_id: int, table_id: int, which: Literal["min", "max"]
) -> int | datetime:
    column_id = SLICE_COLUMNS[database_id][table_id]

    log(f"Consultando {which!r} na tabela '{table_id}' " f"e banco '{database_id}'")
    url = "https://metabase.saude.rj.gov.br/api/dataset/csv"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/x-www-form-urlencoded"}

    dataset_query = {
        "type": "query",
        "database": database_id,
        "query": {
            "source-table": table_id,
            "aggregation": [
                [which, ["+", ["-", ["field", column_id, {"base-type": "type/Text"}], 1], 1]]
            ],
        },
        "parameters": [],
    }

    form_data = {"query": json.dumps(dataset_query, ensure_ascii=False)}

    response = requests.post(url, headers=headers, data=form_data, verify=False)

    res = int(re.search(r"\n(\d+)", response.text).group(1))
    log(f"O valor {which!r} para a coluna usada é '{res}'")

    return res


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def calculate_slices(
    min_value, max_value, which: Literal["min", "max"], slice_size=900_000
) -> list[int]:
    """
    Calcula os intervalos de busca para que não passe de
    `slice_size` registros e retorna uma lista de inteiros
    baseados no valor de `which`
    """
    slices = [
        i + (0 if which == "min" else slice_size) for i in range(min_value, max_value, slice_size)
    ]
    log(f"valor {which!r} dos slices: {slices}")
    return slices


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def query_database_slice(
    token: str,
    database_id: int,
    table_id: int,
    slice_min: int | str,
    slice_max: int | str,
    extraction_date: datetime,
) -> pd.DataFrame:
    """
    Faz a consulta ao metabase no intervalo ditado por `slice_min` e
    `slice_max`
    """
    column_id = SLICE_COLUMNS[database_id][table_id]

    log(
        f"Iniciando consulta ao banco de dados. Database ID: {database_id}, "
        f"Table ID: {table_id}, onde: {slice_min} <= valor < {slice_max}"
    )
    url = "https://metabase.saude.rj.gov.br/api/dataset/csv"
    headers = {"X-Metabase-Session": token, "Content-Type": "application/x-www-form-urlencoded"}

    dataset_query = {
        "type": "query",
        "database": database_id,
        "query": {
            "source-table": table_id,
            "filter": [
                "and",
                [">=", ["field", column_id, {"base-type": "type/Text"}], f"{slice_min}"],
                ["<", ["field", column_id, {"base-type": "type/Text"}], f"{slice_max}"],
            ],
        },
        "parameters": [],
    }

    form_data = {"query": json.dumps(dataset_query, ensure_ascii=False)}

    response = requests.post(url, headers=headers, data=form_data, verify=False)
    csv_file = io.StringIO(response.content.decode("utf-8"))

    df = pd.read_csv(csv_file)

    df["data_extracao"] = extraction_date

    if len(df) == 1_000_000:
        log("Consulta possivelmente truncada")
        raise Exception("Consulta possivelmente truncada")

    log("Consulta ao banco de dados concluída.")

    return df


@task
def get_extraction_datetime() -> datetime:
    return datetime.now()


@task
def upload_df_to_datalake_wrapper(
    df: pd.DataFrame,
    table_id: str,
    dataset_id: str,
):
    if not df.empty:
        upload_df_to_datalake.run(
            df=df,
            table_id=table_id,
            dataset_id=dataset_id,
            partition_column="data_extracao",
            source_format="parquet",
        )
