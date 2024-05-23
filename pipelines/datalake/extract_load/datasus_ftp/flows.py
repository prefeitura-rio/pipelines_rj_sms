# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SISREG dumping flows
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.datasus_ftp.schedules import (
    datasus_weekly_update_schedule,
)
from pipelines.datalake.extract_load.datasus_ftp.tasks import (
    create_many_partitions,
    extract_data_from_datasus,
    transform_data,
    upload_many_to_datalake,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import create_folders

with Flow(name="DataLake - Extração e Carga de Dados - DataSUS") as sms_dump_datasus:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # DatSUS
    ENDPOINT = Parameter("endpoint", required=True)
    DOWNLOAD_NEWEST_FILE = Parameter("download_newest", default=False, required=False)
    # Aditional parameters for CNES if download_newest is False
    FILE = Parameter("file", default=None, required=False)

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            endpoint=ENDPOINT,
            downlaod_newest_file=DOWNLOAD_NEWEST_FILE,
            file=FILE,
        )

    ####################################
    # Tasks section #1 - Extract data
    ####################################
    raw_file = extract_data_from_datasus(
        endpoint=ENDPOINT,
        download_newest=DOWNLOAD_NEWEST_FILE,
        file=FILE,
        download_path=local_folders["raw"],
    )

    #####################################
    # Tasks section #2 - Transform data
    #####################################

    transformed_file = transform_data(files_path=raw_file, endpoint=ENDPOINT)

    #####################################
    # Tasks section #3 - Load data
    #####################################

    create_partitions_task = create_many_partitions(
        files_path=transformed_file,
        partition_directory=local_folders["partition_directory"],
        endpoint=ENDPOINT,
        upstream_tasks=[transformed_file],
    )

    upload_to_datalake_task = upload_many_to_datalake(
        input_path=local_folders["partition_directory"],
        dataset_id=DATASET_ID,
        endpoint=ENDPOINT,
        source_format="parquet",
        if_exists="replace",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[create_partitions_task],
    )

sms_dump_datasus.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_datasus.executor = LocalDaskExecutor(num_workers=1)
sms_dump_datasus.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="8Gi",
)
sms_dump_datasus.schedule = datasus_weekly_update_schedule
