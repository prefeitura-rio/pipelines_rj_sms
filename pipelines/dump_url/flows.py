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
#from pipelines.dump_db_smsrio.schedules import smsrio_daily_update_schedule
from pipelines.utils.tasks import (
    create_folders,
    inject_gcp_credentials,
    download_from_url,
    upload_to_datalake,
)

with Flow(
    name="Dump URL - Ingerir dados de URLs",
) as sms_dump_url:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # URL
    URL_TYPE = Parameter("url_type", required=True, default="google_sheet")
    URL = Parameter("url", required=True)
    GSHEET_SHEET_NAME = Parameter("gsheet_sheet_name", default="Sheet1")

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", required=True)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = task_rename_current_flow_run_dataset_table(
            prefix="Dump URL: ",
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            upstream_tasks=[inject_gcp_credentials_task],
        )

    ####################################
    # Tasks section #1 - Get data
    #####################################
    create_folders_task = create_folders(upstream_tasks=[inject_gcp_credentials_task])

    download_task = download_from_url(
        url=URL,
        file_path=create_folders_task['raw'],
        file_name=TABLE_ID,
        url_type=URL_TYPE,
        gsheets_sheet_name=GSHEET_SHEET_NAME,
        csv_delimiter="|",
        upstream_tasks=[create_folders_task],
    )

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    #upload_to_datalake_task = upload_to_datalake(
    #    input_path=download_task,
    #    dataset_id=DATASET_ID,
    #    table_id=TABLE_ID,
    #    if_exists="replace",
    #    csv_delimiter=";",
    #    if_storage_data_exists="replace",
    #    biglake_table=True,
    #    dataset_is_public=False,
    #    upstream_tasks=[download_from_url],
    #)


#sms_dump_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
#sms_dump_smsrio.executor = LocalDaskExecutor(num_workers=10)
#sms_dump_smsrio.run_config = KubernetesRun(
#    image=constants.DOCKER_IMAGE.value,
#    labels=[
#        constants.RJ_SMS_AGENT_LABEL.value,
#    ],
#    memory_limit="2Gi",
#)
#
#sms_dump_smsrio.schedule = smsrio_daily_update_schedule
