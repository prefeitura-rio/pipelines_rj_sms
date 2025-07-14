# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123, C0301
# flake8: noqa E501
"""
TPC dumping flows
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.tpc_azure_blob.constants import (
    constants as tpc_constants,
)
from pipelines.datalake.extract_load.tpc_azure_blob.schedules import (
    tpc_daily_update_schedule,
)
from pipelines.datalake.extract_load.tpc_azure_blob.tasks import (
    extract_data_from_blob,
    transform_data,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import create_folders, create_partitions, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - TPC",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DANIEL_ID.value],
) as sms_dump_tpc:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # TPC Azure
    BLOB_FILE = Parameter("blob_file", required=True)

    # GCP
    DATASET_ID = Parameter("dataset_id", default=tpc_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    with case(RENAME_FLOW, True):

        rename_current_flow_run(
            environment=ENVIRONMENT,
            blob_file=BLOB_FILE,
        )

    ####################################
    # Tasks section #1 - Extract data
    #####################################
    raw_file = extract_data_from_blob(
        blob_file=BLOB_FILE,
        file_folder=local_folders["raw"],
        file_name=TABLE_ID,
        environment=ENVIRONMENT,
    )

    ####################################
    # Tasks section #2 - Validate data
    #####################################

    transformed_file = transform_data(
        file_path=raw_file,
        blob_file=BLOB_FILE,
    )

    #####################################
    # Tasks section #4 - Load data
    #####################################

    create_partitions_task = create_partitions(
        data_path=local_folders["raw"],
        partition_directory=local_folders["partition_directory"],
        upstream_tasks=[transformed_file],
    )

    upload_to_datalake_task = upload_to_datalake(
        input_path=local_folders["partition_directory"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[create_partitions_task],
    )


sms_dump_tpc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_tpc.executor = LocalDaskExecutor(num_workers=1)
sms_dump_tpc.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)
sms_dump_tpc.schedule = tpc_daily_update_schedule
