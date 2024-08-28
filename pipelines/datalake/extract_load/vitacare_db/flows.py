# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
VitaCare dumping flows
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.prontuarios.utils.tasks import get_healthcenter_name_from_cnes
from pipelines.datalake.extract_load.vitacare_db.tasks import (
    get_bucket_name,
    download_from_cloud_storage_task,
    transform_data,
    upload_many_to_datalake,
)

from pipelines.utils.tasks import create_folders

with Flow(name="DataLake - Extração e Carga de Dados - VitaCare DB") as sms_dump_vitacare_db:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # VITACARE DB
    CNES = Parameter("cnes", default=None, required=True)

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    bucket_name = get_bucket_name(env=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        healthcenter_name = get_healthcenter_name_from_cnes(cnes=CNES)

        rename_current_flow_run(
            environment=ENVIRONMENT,
            unidade=healthcenter_name,
            cnes=CNES,
        )

    ####################################
    # Tasks section #1 - Extract data
    ####################################
    raw_file = download_from_cloud_storage_task(
        path=local_folders["raw"],
        bucket_name=bucket_name,
        blob_prefix=CNES,
        upstream_tasks=[create_folders, bucket_name],
    )

    #####################################
    # Tasks section #2 - Transform data
    #####################################

    transformed_file = transform_data(files_path=raw_file)

    #####################################
    # Tasks section #3 - Load data
    #####################################

    upload_to_datalake_task = upload_many_to_datalake(
        input_path=transformed_file,
        dataset_id=DATASET_ID,
        source_format="parquet",
        if_exists="replace",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[transformed_file],
    )
    upload_to_datalake_task.set_upstream(transformed_file)


sms_dump_vitacare_db.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_db.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare_db.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)
