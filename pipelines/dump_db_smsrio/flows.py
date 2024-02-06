# -*- coding: utf-8 -*-
# pylint: disable=C0103
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
from pipelines.dump_db_smsrio.constants import constants as smsrio_constants

# from pipelines.dump_api_vitai.schedules import vitai_daily_update_schedule
from pipelines.dump_db_smsrio.tasks import download_from_db, build_gcp_table
from pipelines.utils.tasks import (
    create_folders,
    get_secret_key,
    inject_gcp_credentials,
    upload_to_datalake,
)

with Flow(
    name="Dump SMS Rio - Ingerir dados da plataforma SMSRio",
) as sms_dump_smsrio:
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

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=smsrio_constants.DATASET_ID.value)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = task_rename_current_flow_run_dataset_table(
            prefix="Dump SMS Rio: ",
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            upstream_tasks=[inject_gcp_credentials_task],
        )

    ####################################
    # Tasks section #1 - Get data
    #####################################
    get_secret_task = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_DBURL,
        environment="prod",
        upstream_tasks=[inject_gcp_credentials_task],
    )

    create_folders_task = create_folders(upstream_tasks=[get_secret_task])

    download_task = download_from_db(
        db_url=get_secret_task,
        db_schema=smsrio_constants.SCHEMA_ID.value,
        db_table=TABLE_ID,
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID,
        upstream_tasks=[create_folders_task],
    )

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################
    build_gcp_table_task = build_gcp_table(db_table=TABLE_ID, upstream_tasks=[download_task])

    upload_to_datalake_task = upload_to_datalake(
        input_path=download_task,
        dataset_id=DATASET_ID,
        table_id=build_gcp_table_task,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[build_gcp_table_task],
    )


sms_dump_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_smsrio.executor = LocalDaskExecutor(num_workers=10)
sms_dump_smsrio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

# sms_dump_smsrio.schedule = vitai_daily_update_schedule
