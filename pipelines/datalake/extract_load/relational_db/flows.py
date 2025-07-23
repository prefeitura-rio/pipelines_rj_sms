# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123, C0301
# flake8: noqa E501
"""
Uploading patient data to datalake
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.relational_db.schedules import schedules
from pipelines.datalake.extract_load.relational_db.tasks import (
    build_gcp_table,
    download_from_db,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake
from pipelines.utils.time import from_relative_date, get_datetime_working_range

with Flow(
    name="DataLake - Extração e Carga de Dados - Banco de Dados Relacional",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.PEDRO_ID.value,
    ],
) as extract_load_relational_db:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    DB_URL_INFISICAL_KEY = Parameter("db_url_infisical_key", required=True)
    DB_URL_INFISICAL_PATH = Parameter("db_url_infisical_path", required=True)

    SOURCE_SCHEMA_NAME = Parameter("source_schema_name", required=True)
    SOURCE_TABLE_NAME = Parameter("source_table_name", required=True)
    SOURCE_DATETIME_COLUMN = Parameter("source_datetime_column", default="created_at")
    RELATIVE_DATETIME = Parameter("relative_datetime", default="D-1")
    HISTORICAL_MODE = Parameter("historical_mode", default=False)

    # Target
    TARGET_DATASET_ID = Parameter("target_dataset_id", required=True)
    TARGET_TABLE_ID = Parameter("target_table_id", required=True)

    #####################################
    # Set environment
    ####################################
    build_gcp_table_task = build_gcp_table(
        db_table=SOURCE_TABLE_NAME,
        schema=SOURCE_SCHEMA_NAME,
    )

    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT, table=TARGET_TABLE_ID)

    interval_start, interval_end = get_datetime_working_range(
        start_datetime=from_relative_date(relative_date=RELATIVE_DATETIME),
        return_as_str=True,
        timezone="America/Sao_Paulo",
    )

    ####################################
    # Tasks section #1 - Get data
    #####################################
    database_url = get_secret_key(
        secret_path=DB_URL_INFISICAL_PATH,
        secret_name=DB_URL_INFISICAL_KEY,
        environment=ENVIRONMENT,
    )

    dataframe = download_from_db(
        db_url=database_url,
        db_table=SOURCE_TABLE_NAME,
        db_schema=SOURCE_SCHEMA_NAME,
        start_target_date=interval_start,
        end_target_date=interval_end,
        historical_mode=HISTORICAL_MODE,
        reference_datetime_column=SOURCE_DATETIME_COLUMN,
    )
    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################
    upload_df_to_datalake(
        df=dataframe,
        dataset_id=TARGET_DATASET_ID,
        table_id=build_gcp_table(
            db_table=SOURCE_TABLE_NAME,
            schema=SOURCE_SCHEMA_NAME,
        ),
        source_format="parquet",
        partition_column="loaded_at",
    )


extract_load_relational_db.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_load_relational_db.executor = LocalDaskExecutor(num_workers=2)
extract_load_relational_db.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="8Gi",
)
extract_load_relational_db.schedule = schedules
