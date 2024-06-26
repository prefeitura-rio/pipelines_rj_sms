# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SMSRio dumping flows
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_rename_current_flow_run_dataset_table,
)

from pipelines.constants import constants
from pipelines.datalake.extract_load.smsrio_mysql_general.constants import (
    constants as smsrio_constants,
)
from pipelines.datalake.extract_load.smsrio_mysql_general.schedules import (
    smsrio_daily_update_schedule,
)
from pipelines.datalake.extract_load.smsrio_mysql_general.tasks import (
    build_gcp_table,
    download_from_db,
)
from pipelines.utils.tasks import (
    create_folders,
    get_secret_key,
    upload_to_datalake,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - SMS Rio Plataforma (genérico)",
) as sms_dump_smsrio_general:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # INFISICAL
    INFISICAL_PATH = smsrio_constants.INFISICAL_PATH.value
    INFISICAL_DBURL = smsrio_constants.INFISICAL_DB_URL.value

    # SMSRio DB
    TABLE_ID = Parameter("table_id", required=True)
    SCHEMA = Parameter("schema", required=True)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=smsrio_constants.DATASET_ID.value)

    #####################################
    # Set environment
    ####################################

    build_gcp_table_task = build_gcp_table(
        db_table=TABLE_ID
    )

    with case(RENAME_FLOW, True):
        rename_flow_task = task_rename_current_flow_run_dataset_table(
            prefix="Dump SMS Rio: ",
            dataset_id=DATASET_ID,
            table_id=build_gcp_table_task
        )

    ####################################
    # Tasks section #1 - Get data
    #####################################
    get_secret_task = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_DBURL,
        environment="prod"
    )

    create_folders_task = create_folders()

    download_task = download_from_db(
        db_url=get_secret_task,
        db_schema=SCHEMA,
        db_table=TABLE_ID,
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID
    )

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    upload_to_datalake_task = upload_to_datalake(
        input_path=download_task,
        dataset_id=DATASET_ID,
        table_id=build_gcp_table_task,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False
    )


sms_dump_smsrio_general.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_smsrio_general.executor = LocalDaskExecutor(num_workers=10)
sms_dump_smsrio_general.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

# sms_dump_smsrio.schedule = smsrio_general_daily_update_schedule
