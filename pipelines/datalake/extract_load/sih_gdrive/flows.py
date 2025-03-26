# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
"""
SIH dumping flows
"""
# from datetime import timedelta

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.sih_gdrive.constants import (
    constants as sih_constants,
)

# from pipelines.datalake.extract_load.sih_gdrive.schedules import daily_update_schedule
from pipelines.datalake.extract_load.sih_gdrive.tasks import (
    generate_filters,
    transform_data,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.google_drive import dowload_from_gdrive
from pipelines.utils.tasks import create_folders, create_partitions, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - SIH",
) as sms_dump_sih:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # GOOGLE DRIVE
    LAST_UPDATE_START_DATE = Parameter("last_update_start_date", default=None)
    LAST_UPDATE_END_DATE = Parameter("last_update_end_date", default=None)

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

    filters = generate_filters(
        last_update_start_date=LAST_UPDATE_START_DATE, last_update_end_date=LAST_UPDATE_END_DATE
    )

    raw_files = dowload_from_gdrive(
        folder_id=sih_constants.GDRIVE_FOLDER_ID.value,
        destination_folder=local_folders["raw"],
        filter_type="last_updated",
        filter_param=filters,
    )

    #####################################
    # Tasks section #2 - Transform data
    #####################################

    with case(raw_files["has_data"], True):
        transformed_files = transform_data(files_path=raw_files["files"])

        #####################################
        # Tasks section #3 - Load data
        #####################################

        create_partitions_task = create_partitions(
            data_path=transformed_files,
            partition_directory=local_folders["partition_directory"],
            file_type="parquet",
        )

        upload_to_datalake_task = upload_to_datalake(
            input_path=local_folders["partition_directory"],
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dump_mode="append",
            source_format="parquet",
            if_exists="replace",
            if_storage_data_exists="replace",
            biglake_table=True,
            dataset_is_public=False,
        )
        upload_to_datalake_task.set_upstream(create_partitions_task)


sms_dump_sih.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_sih.executor = LocalDaskExecutor(num_workers=1)
sms_dump_sih.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)
# sms_dump_sih.schedule = daily_update_schedule
