# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SMSRio dumping flows
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.prefect import (
    task_rename_current_flow_run_dataset_table,
)

from pipelines.constants import constants
from pipelines.datalake.extract_load.google_sheets.schedules import (
    daily_update_schedule,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    create_folders,
    download_from_url,
    inject_gcp_credentials,
    upload_to_datalake,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Google Sheets",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DIT_ID.value,
    ],
) as sms_dump_url:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # URL
    URL_TYPE = Parameter("url_type", required=True, default="google_sheet")
    URL = Parameter("url", required=True)
    GSHEETS_SHEET_NAME = Parameter("gsheets_sheet_name")
    CSV_DELIMITER = Parameter("csv_delimiter", default=";")

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
        file_path=create_folders_task["raw"],
        file_name=TABLE_ID,
        url_type=URL_TYPE,
        gsheets_sheet_name=GSHEETS_SHEET_NAME,
        csv_delimiter=CSV_DELIMITER,
        upstream_tasks=[create_folders_task],
    )

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    upload_to_datalake_task = upload_to_datalake(
        input_path=create_folders_task["raw"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        if_exists="replace",
        csv_delimiter=CSV_DELIMITER,
        dump_mode="overwrite",
        delete_mode="staging",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[download_task],
    )


sms_dump_url.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_url.executor = LocalDaskExecutor(num_workers=10)
sms_dump_url.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

sms_dump_url.schedule = daily_update_schedule
