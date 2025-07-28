# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SMSRio dumping flows
"""

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.smsrio_mysql.schedules import (
    smsrio_daily_update_schedule,
)
from pipelines.datalake.extract_load.smsrio_mysql.tasks import (
    build_bq_table_name,
    create_extraction_batches,
    download_from_db,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake
from pipelines.utils.time import from_relative_date

with Flow(
    name="DataLake - Extração e Carga de Dados - SMS Rio Plataforma",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DIT_ID.value,
        constants.MATHEUS_ID.value,
    ],
) as sms_dump_smsrio:
    #####################################
    # Parameters
    #####################################

    # INFISICAL
    INFISICAL_PATH = Parameter("infisical_path", default="/smsrio")

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # SMSRio DB
    TABLE_ID = Parameter("table_id", required=True)
    SCHEMA = Parameter("schema", required=True)
    DATETIME_COLUMN = Parameter("datetime_column", default="timestamp")
    ID_COLUMN = Parameter("id_column", default="id")

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default="brutos_plataforma_smsrio")

    # Storage Configuration
    RELATIVE_DATE_FILTER = Parameter("relative_date_filter", default=None)
    PARTITION_COLUMN = Parameter("partition_column", default=None)

    #####################################
    # Set environment
    ####################################
    bq_table_name = build_bq_table_name(db_table=TABLE_ID, schema=SCHEMA)

    date_filter = from_relative_date(relative_date=RELATIVE_DATE_FILTER)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT, dataset=DATASET_ID, table=bq_table_name)

    ####################################
    # Tasks section #1 - Get data
    #####################################
    DB_URL = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name="DB_URL", environment=ENVIRONMENT
    )

    queries = create_extraction_batches(
        db_url=DB_URL,
        db_schema=SCHEMA,
        db_table=TABLE_ID,
        date_filter=date_filter,
        datetime_column=DATETIME_COLUMN,
        id_column=ID_COLUMN,
    )

    dataframes = download_from_db.map(
        db_url=unmapped(DB_URL),
        query=queries,
    )

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################
    upload_df_to_datalake.map(
        df=dataframes,
        dataset_id=unmapped(DATASET_ID),
        table_id=unmapped(bq_table_name),
        partition_column=unmapped(PARTITION_COLUMN),
        source_format=unmapped("parquet"),
        if_exists=unmapped("append"),
        if_storage_data_exists=unmapped("append"),
    )


sms_dump_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_smsrio.executor = LocalDaskExecutor(num_workers=10)
sms_dump_smsrio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
    memory_request="5Gi",
)

sms_dump_smsrio.schedule = smsrio_daily_update_schedule
