# -*- coding: utf-8 -*-
"""
Flow
"""
# Prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# Internos
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.ser_metabase.schedules import schedule
from pipelines.datalake.extract_load.ser_metabase.tasks import (
    authenticate_in_metabase,
    calculate_slices,
    get_extraction_datetime,
    query_database_slice,
    query_slice_limit,
    upload_df_to_datalake_wrapper,
)
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.tasks import get_secret_key

with Flow("SUBGERAL - Extract & Load - SER METABASE") as ser_metabase_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # METABASE ------------------------------
    DATABASE_ID = Parameter("database_id", default=178, required=True)
    TABLE_ID = Parameter("table_id", default=3255, required=True)

    # BIGQUERY ------------------------------
    BQ_DATASET_ID = Parameter("bq_dataset_id", default="brutos_ser_metabase", required=True)
    BQ_TABLE_ID = Parameter("bq_table_id", default="FATO_AMBULATORIO", required=True)

    # SLICING -------------------------------
    SLICE_SIZE = Parameter("slice_size", default=900_000, required=False)

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

    # Task 2 - Calculate minimal value for data slicing
    min_value = query_slice_limit(
        token=token, database_id=DATABASE_ID, table_id=TABLE_ID, which="min"
    )
    # Task 3 - Calculate maximal value for data slicing
    max_value = query_slice_limit(
        token=token, database_id=DATABASE_ID, table_id=TABLE_ID, which="max"
    )

    # Task 4 - Calculate list of inital values for each slice
    slices_min = calculate_slices(
        min_value=min_value,
        max_value=max_value,
        which="min",
        slice_size=SLICE_SIZE,
    )
    # Task 5 - Calculate list of final values for each slice
    slices_max = calculate_slices(
        min_value=min_value,
        max_value=max_value,
        which="max",
        slice_size=SLICE_SIZE,
    )

    # Task 6 - Queries the data for each slice determined
    extraction_date = get_extraction_datetime()

    # Task 7 - Queries the data for each slice determined
    dfs = query_database_slice.map(
        token=unmapped(token),
        database_id=unmapped(DATABASE_ID),
        table_id=unmapped(TABLE_ID),
        extraction_date=unmapped(extraction_date),
        slice_min=slices_min,
        slice_max=slices_max,
    )

    # Task 8 - Transform Data Frame columns
    dfs_columns_ok = handle_columns_to_bq.map(df=dfs)

    # Task 9 - Upload to Big Query
    upload_df_to_datalake_wrapper.map(
        df=dfs_columns_ok,
        table_id=BQ_TABLE_ID,
        dataset_id=BQ_DATASET_ID,
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
