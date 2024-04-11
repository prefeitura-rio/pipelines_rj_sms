# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
SISREG dumping flows
"""
from prefect import Parameter, case
from prefect.run_configs import VertexRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.utils.tasks import create_folders, upload_to_datalake, create_partitions
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.datalake.extract_load.sisreg_web.constants import constants as sisreg_constants
from pipelines.datalake.extract_load.sisreg_web.tasks import (
    extract_data_from_sisreg,
    transform_data,
)

with Flow(name="DataLake - Extração e Carga de Dados - Sisreg") as sms_dump_sisreg:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)
    REPROCESS_MODE = Parameter("reprocess_mode", default=False)

    # Sisreg
    ENDPOINT = Parameter("endpoint", default="escala")

    # GCP
    DATASET_ID = Parameter("dataset_id", default=sisreg_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            endpoint=ENDPOINT,
        )

    ####################################
    # Tasks section #1 - Extract data
    ####################################
    raw_file = extract_data_from_sisreg(
        environment=ENVIRONMENT, endpoint=ENDPOINT, download_path=local_folders["raw"]
    )

    #####################################
    # Tasks section #3 - Transform data
    #####################################

    transformed_file = transform_data(file_path=raw_file, endpoint=ENDPOINT)

    #####################################
    # Tasks section #4 - Load data
    #####################################

    create_partitions_task = create_partitions(
        data_path=transformed_file,
        partition_directory=local_folders["partition_directory"],
        upstream_tasks=[transformed_file],
    )

    #upload_to_datalake_task = upload_to_datalake(
    #    input_path=local_folders["partition_directory"],
    #    dataset_id=DATASET_ID,
    #    table_id=TABLE_ID,
    #    if_exists="replace",
    #    if_storage_data_exists="replace",
    #    biglake_table=True,
    #    dataset_is_public=False,
    #    upstream_tasks=[create_partitions_task],
    #)


# Storage and run configs
sms_dump_sisreg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_sisreg.run_config = VertexRun(
    image=constants.DOCKER_VERTEX_IMAGE.value,
    labels=[
        constants.RJ_SMS_VERTEX_AGENT_LABEL.value,
    ],
    # https://cloud.google.com/vertex-ai/docs/training/configure-compute#machine-types
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": constants.INFISICAL_TOKEN.value,
    },
)
