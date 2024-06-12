# -*- coding: utf-8 -*-
import asyncio
from datetime import timedelta
from math import ceil

import httpx
from httpx import AsyncClient

from pipelines.prontuarios.mrg.functions import (
    final_merge,
    first_merge,
    load_ranking,
    sanity_check,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import get_secret_key


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def build_db_url(environment: str):
    # host = get_secret_key.run(secret_path="/", secret_name="DB_IP", environment=environment)
    host = get_secret_key.run(secret_path="/", secret_name="DB_IP", environment="dev")

    user = get_secret_key.run(secret_path="/", secret_name="DB_USER", environment=environment)
    password = get_secret_key.run(
        secret_path="/", secret_name="DB_PASSWORD", environment=environment
    )
    database = get_secret_key.run(secret_path="/", secret_name="DB_NAME", environment=environment)

    return f"postgresql://{user}:{password}@{host}:5432/{database}"


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def build_param_list(
    batch_size: int,
    environment: str,
    db_url: str
):
    query = """
        SELECT COUNT(DISTINCT patient_cpf) as total
        FROM std__patientrecord
    """
    try:
        results = pd.read_sql(query, db_url)
    except Exception as e:
        log(f"Error extracting data from database: {e}", level="error")
        raise e

    count = results["total"].values[0]
    batch_count = ceil(count / batch_size)

    params = []
    for i in range(batch_count):
        params.append(
            {
                "enviroment": environment,
                "rename_flow": True,
                "limit": batch_size,
                "offset": i * batch_size,
            }
        )

    return params


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def get_mergeable_data_from_db(limit: int, offset: int, db_url: str) -> pd.DataFrame:

    query = f"""
        SELECT std.*,
            d.system,
            rp.source_updated_at as event_moment,
            rp.created_at as ingestion_moment
        FROM std__patientrecord as std
            LEFT JOIN raw__patientrecord as rp
                ON rp.id = std.raw_source_id
            LEFT JOIN datasource as d
                ON d.cnes = rp.data_source_id
        WHERE std.patient_cpf in
        (
            SELECT DISTINCT patient_cpf
            FROM std__patientrecord
            LIMIT {limit}
            OFFSET {offset}
        )
    """
    try:
        mergeable_data = pd.read_sql(query, db_url)
    except Exception as e:
        log(f"Error extracting data from database: {e}", level="error")
        raise e

    return mergeable_data


@task
def merge_patientrecords(mergeable_data: pd.DataFrame):

    mergeable_data.loc[mergeable_data["birth_date"].notna(), "birth_date"] = mergeable_data.loc[
        mergeable_data["birth_date"].notna(), "birth_date"
    ].astype(str)

    mergeable_data.loc[mergeable_data["deceased_date"].notna(), "deceased_date"] = (
        mergeable_data.loc[mergeable_data["deceased_date"].notna(), "deceased_date"].astype(str)
    )

    grouped = mergeable_data.groupby("patient_code").apply(lambda x: x.to_dict(orient="records"))

    # -------------------
    # MERGE PROCESS
    # -------------------

    def normalize_payload_list(register_list: dict) -> pd.DataFrame:
        return pd.DataFrame(register_list)

    data_to_merge = grouped.values

    log(f"Merging data for {len(data_to_merge)} patients")
    register_list = list(map(normalize_payload_list, data_to_merge))

    log("Trying first merge")
    ranking_df = load_ranking()
    merged_n_not_merged_data = list(map(lambda x: first_merge(x, ranking_df), register_list))

    log("Final merge")
    merged_data = list(map(final_merge, merged_n_not_merged_data))

    log("Sanity check")
    sanitized_data = list(map(sanity_check, merged_data))

    return sanitized_data


@task(max_retries=3, retry_delay=timedelta(minutes=2))
def send_merged_data_to_api(merged_data: list[dict], api_url: str, api_token: str):
    # Remove null fields from dict
    merged_data = [{k: v for k, v in record.items() if v is not None} for record in merged_data]

    # Send payload
    async def send():
        async with AsyncClient(timeout=500) as client:
            try:
                response = await client.put(
                    url=f"{api_url}mrg/patient",
                    headers={"Authorization": f"Bearer {api_token}"},
                    json=merged_data,
                )
            except httpx.ReadTimeout:
                raise
            return response

    response = asyncio.run(send())

    # Decision based on response
    if response.status_code in [200, 201]:
        log("Data sent successfully")
        return response
    else:
        log(f"Error sending data to API: {response.text}", level="error")
        raise Exception(f"Error sending data to API: {response.text}")
