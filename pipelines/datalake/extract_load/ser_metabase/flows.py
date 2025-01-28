# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.ser_metabase.schedules import schedule
from pipelines.datalake.extract_load.ser_metabase.tasks import (
    authenticate_in_metabase,
    convert_metabase_json_to_df,
    query_database,
    save_data,
)
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow("Extract Load: Ser Metabase") as ser_metabase_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    DATABASE_ID = Parameter("database_id", default=178, required=True)
    TABLE_ID = Parameter("table_id", default=3477, required=True)

    # ------------------------------
    # Section 1 - Authenticate in Metabase
    # ------------------------------
    user = get_secret_key(environment=ENVIRONMENT, secret_name="USER", secret_path="/metabase")
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="PASSWORD", secret_path="/metabase"
    )

    token = authenticate_in_metabase(
        user=user,
        password=password,
    )

    # ------------------------------
    # Section 2 - Query Database
    # ------------------------------
    json_res = query_database(
        token=token,
        database_id=DATABASE_ID,
        table_id=TABLE_ID,
    )

    # ------------------------------
    # Section 3 - Save Data
    # ------------------------------
    df = convert_metabase_json_to_df(json_res=json_res)

    upload_df_to_datalake(
        df=df,
        table_id="ser_metabase",
        dataset_id="brutos_plataforma_ser",
        partition_column="data_extracao",
        source_format="parquet",
    )

ser_metabase_flow.executor = LocalDaskExecutor(num_workers=1)
ser_metabase_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
ser_metabase_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
ser_metabase_flow.schedule = schedule
