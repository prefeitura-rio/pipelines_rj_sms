# -*- coding: utf-8 -*-
"""
Flow
"""
# Prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# Internos
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

from pipelines.constants import constants
from pipelines.datalake.extract_load.ser_metabase.schedules import schedule
from pipelines.datalake.extract_load.ser_metabase.tasks import (
    authenticate_in_metabase,
    interrupt_if_empty,
    query_database,
)
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow("SUBGERAL - Extract & Load - SER METABASE",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
    ],
) as ser_metabase_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # METABASE ------------------------------
    DATABASE_ID = Parameter("database_id", default=178, required=True)
    TABLE_ID = Parameter("table_id", default=3255, required=True)

    # BIGQUERY ------------------------------
    BQ_DATASET_ID = Parameter("bq_dataset_id", default="brutos_ser_metabase", required=True)
    BQ_TABLE_ID = Parameter("bq_table_id", default="FATO_AMBULATORIO", required=True)

    # CREDENTIALS ------------------------------
    user = get_secret_key(environment=ENVIRONMENT, secret_name="USER", secret_path="/metabase")
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="PASSWORD", secret_path="/metabase"
    )

    # Task 1 - Authenticate in Metabase
    token = authenticate_in_metabase(
        user=user,
        password=password,
    )

    # Task 2 - Query Database
    df = query_database(
        token=token,
        database_id=DATABASE_ID,
        table_id=TABLE_ID,
    )

    # Task 3 - Verify Database length
    df_verified = interrupt_if_empty(df=df)

    # Task 4 - Transform Data Frame columns
    df_columns_ok = handle_columns_to_bq(df=df_verified)

    # Task 5 - Upload to Big Query
    upload_df_to_datalake(
        df=df_columns_ok,
        table_id=BQ_TABLE_ID,
        dataset_id=BQ_DATASET_ID,
        partition_column="data_extracao",
        source_format="parquet",
    )

# ------------------------------------

ser_metabase_flow.executor = LocalDaskExecutor(num_workers=1)
ser_metabase_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
ser_metabase_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="10Gi",
    memory_limit="10Gi",
)
ser_metabase_flow.schedule = schedule
