# -*- coding: utf-8 -*-

from datetime import datetime, timedelta

import pandas as pd
import pytz
from prefect.triggers import all_finished
from sqlalchemy import create_engine

from pipelines.datalake.migrate.gcs_to_cloudsql import utils as cloudsql_utils
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(minutes=2))
def process_rnds_table(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_schema: str,
    ap: str,
    dataset_id: str,
    partition_column: str,
):
    table_suffix = "RNDS_RIA_ROUTINE_IMMUNIZATION_CONTROL"
    table_name = f"{ap}_{table_suffix}"
    full_table_name = f"{db_schema}.{table_name}"

    connection_string = (
        f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/rnds_historic"
        "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    )

    engine = create_engine(connection_string)

    query = f"SELECT * FROM {full_table_name}"

    total_rows = 0
    is_first_chunk = True

    chunks = pd.read_sql(query, engine, chunksize=500000)

    now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)

    for chunk in chunks:
        if chunk.empty:
            continue

        chunk["extracted_at"] = now

        upload_df_to_datalake.run(
            df=chunk,
            dataset_id=dataset_id,
            table_id=table_name.lower(),
            partition_column=partition_column,
            source_format="parquet",
            if_exists="replace" if is_first_chunk else "append",
            if_storage_data_exists="replace" if is_first_chunk else "append",
        )

        total_rows += len(chunk)
        is_first_chunk = False

    return total_rows


@task
def start_rnds_instance(instance_name: str):
    log("[start_rnds_instance] Guaranteeing instance is running")

    always_on = {"settings": {"activationPolicy": "ALWAYS"}}
    cloudsql_utils.call_api("PATCH", f"/instances/{instance_name}", json=always_on)
    cloudsql_utils.wait_for_operations(instance_name, label="turning on")
    cloudsql_utils.get_instance_status(instance_name)


@task(trigger=all_finished)
def stop_rnds_instance(instance_name: str):
    log("[stop_rnds_instance] Stopping instance")

    always_off = {"settings": {"activationPolicy": "NEVER"}}
    cloudsql_utils.call_api("PATCH", f"/instances/{instance_name}", json=always_off)
    cloudsql_utils.wait_for_operations(instance_name, label="turning off")
    cloudsql_utils.get_instance_status(instance_name)
