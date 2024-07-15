# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
"""
Seguir em Frente dumping flows
"""
# from datetime import timedelta

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.seguir_em_frente_pipefy.schedules import (
    daily_update_schedule,
)
from pipelines.datalake.extract_load.seguir_em_frente_pipefy.tasks import (
    download_from_pipefy,
    transform_data,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import create_folders, create_partitions, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - Seguir em Frente",
) as sms_dump_seguir_em_frente:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # Pipefy
    ENDPOINT = Parameter("endpoint", required=True)

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    with case(RENAME_FLOW, True):

        rename_current_flow_run(
            endpoint=ENDPOINT,
            environment=ENVIRONMENT,
        )

    ####################################
    # Tasks section #1 - Extract data
    ####################################

    raw_files = download_from_pipefy(
        endpoint=ENDPOINT, destination_folder=local_folders["raw"], environment=ENVIRONMENT
    )

    #####################################
    # Tasks section #2 - Transform data
    #####################################

    transformed_files = transform_data(files_path=raw_files)

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


sms_dump_seguir_em_frente.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_seguir_em_frente.executor = LocalDaskExecutor(num_workers=1)
sms_dump_seguir_em_frente.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)
sms_dump_seguir_em_frente.schedule = daily_update_schedule
