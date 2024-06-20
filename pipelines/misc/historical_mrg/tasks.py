# -*- coding: utf-8 -*-
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery

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
    host = get_secret_key.run(secret_path="/", secret_name="DB_IP", environment=environment)
    user = get_secret_key.run(secret_path="/", secret_name="DB_USER", environment=environment)
    password = get_secret_key.run(
        secret_path="/", secret_name="DB_PASSWORD", environment=environment
    )
    database = get_secret_key.run(secret_path="/", secret_name="DB_NAME", environment=environment)

    return f"postgresql://{user}:{password}@{host}:5432/{database}"


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def build_param_list(batch_size: int, environment: str, db_url: str, progress_table: pd.DataFrame):
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

    if progress_table.empty:
        skippable_offsets = []
        log("No progress found")
    else:
        skippable_offsets = progress_table[progress_table.limit == batch_size].offset.to_list()
        log(f"Progress found: {len(skippable_offsets)} to skip")

    all_offsets = list(range(0, count, batch_size))
    planned_offsets = [offset for offset in all_offsets if offset not in skippable_offsets]

    params_df = pd.DataFrame(
        {
            "environment": environment,
            "rename_flow": True,
            "limit": batch_size,
            "offset": planned_offsets,
        }
    )

    return params_df.to_dict(orient="records")


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


@task(nout=4)
def merge_patientrecords(mergeable_data: pd.DataFrame):
    mergeable_data.loc[mergeable_data["birth_date"].notna(), "birth_date"] = mergeable_data.loc[
        mergeable_data["birth_date"].notna(), "birth_date"
    ].astype(str)

    mergeable_data.loc[mergeable_data["deceased_date"].notna(), "deceased_date"] = (
        mergeable_data.loc[mergeable_data["deceased_date"].notna(), "deceased_date"].astype(str)
    )

    # Rename columns to routine format
    mergeable_data.rename(
        columns={
            "birth_state_id": "birth_state_cod",
            "birth_city_id": "birth_city_cod",
            "birth_country_id": "birth_country_cod",
        },
        inplace=True,
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
    patient_data = list(map(sanity_check, merged_data))

    # Remove null fields from dict
    patient_data = [{k: v for k, v in record.items() if v is not None} for record in patient_data]

    # Splitting Entities
    addresses, telecoms, cnss = [], [], []
    for record in patient_data:
        # Address
        address_list = record.pop("address_list", [])
        for address in address_list:
            address["patient_code"] = record["patient_code"]
        addresses.extend(address_list)

        # Telecom
        telecom_list = record.pop("telecom_list", [])
        for telecom in telecom_list:
            telecom["patient_code"] = record["patient_code"]
        telecoms.extend(telecom_list)

        # CNS
        cns_list = record.pop("cns_list", [])
        for cns in cns_list:
            cns["patient_code"] = record["patient_code"]
        cnss.extend(cns_list)

    # Rename back columns to DB format
    for patient in patient_data:
        for birth_col in ["birth_state_cod", "birth_city_cod", "birth_country_cod"]:
            if birth_col in patient:
                patient[birth_col.replace("cod", "id")] = patient.pop(birth_col, None)

    return patient_data, addresses, telecoms, cnss


@task()
def save_progress(limit: int, offset: int, environment: str):
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")
    table = bq_client.get_table("rj-sms.gerenciamento__mrg_historico.status")

    errors = bq_client.insert_rows_json(
        table,
        [
            {
                "limit": limit,
                "offset": offset,
                "environment": environment,
                "moment": datetime.now().isoformat(),
            }
        ],
    )

    if errors == []:
        log("Inserted row")
    else:
        log(f"Errors: {errors}", level="error")
        raise ValueError(f"Errors: {errors}")


@task()
def get_progress_table(environment: str):
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

    try:
        bq_client.get_table("rj-sms.gerenciamento__mrg_historico.status")
    except Exception:
        log("Table not found")
        return None

    query = f"""
        SELECT *
        FROM rj-sms.gerenciamento__mrg_historico.status
        WHERE environment = '{environment}'
    """
    all_records = bq_client.query(query).result().to_dataframe()
    return all_records
