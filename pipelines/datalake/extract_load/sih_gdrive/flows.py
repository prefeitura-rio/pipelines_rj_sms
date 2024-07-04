# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
"""
SIH dumping flows
"""
# from datetime import timedelta
from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.sih_gdrive.constants import (
    constants as sih_constants,
)
from pipelines.datalake.extract_load.sih_gdrive.tasks import (
    dowload_from_gdrive,
    transform_data,
)
from pipelines.datalake.extract_load.vitacare_api.tasks import create_partitions
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import create_folders, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - SIH",
) as sms_dump_sih:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # SIH

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    with case(RENAME_FLOW, True):

        rename_current_flow_run(
            environment=ENVIRONMENT,
        )

    ####################################
    # Tasks section #1 - Extract data
    ####################################

    raw_files = dowload_from_gdrive(
        folder_id=sih_constants.GDRIVE_FOLDER_ID.value, destination_folder=local_folders["raw"]
    )

    #####################################
    # Tasks section #2 - Transform data
    #####################################

    transformed_file = transform_data(file_path=raw_folder, env=ENVIRONMENT)

    #####################################
    # Tasks section #3 - Load data
    #####################################

    # create_partitions_task = create_partitions(
    #    data_path=raw_folder,
    #    partition_directory="/Users/thiagotrabach/projects/pipelines_rj_sms/data/partition_directory",
    #    file_type="parquet",
    #    upstream_tasks=[transformed_file],
    # )
#
# upload_to_datalake_task = upload_to_datalake(
#    input_path="/Users/thiagotrabach/projects/pipelines_rj_sms/data/partition_directory",
#    dataset_id=DATASET_ID,
#    table_id=TABLE_ID,
#    dump_mode="append",
#    source_format="parquet",
#    if_exists="replace",
#    if_storage_data_exists="replace",
#    biglake_table=True,
#    dataset_is_public=False,
#    upstream_tasks=[create_partitions_task],
# )
