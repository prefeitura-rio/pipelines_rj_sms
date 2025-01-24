# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SISREG dumping flows
"""
import os

from prefeitura_rio.pipelines_utils.logging import log

from prefect import Parameter, case
from prefect.run_configs import VertexRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.sisreg_web_v2.constants import (
    constants as sisreg_constants,
)
from pipelines.datalake.extract_load.sisreg_web_v2.schedules import (
    sisreg_daily_update_schedule,
)
from pipelines.datalake.extract_load.sisreg_web_v2.tasks import (
    extract_data_from_sisreg,
    transform_data,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import create_folders, create_partitions, upload_to_datalake

with Flow(name="DataLake - Extração e Carga de Dados - Sisreg V. 2") as sms_dump_sisreg:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # Sisreg
    ENDPOINT = Parameter("endpoint", default="oferta_programada")

    # GCP
    DATASET_ID = Parameter("dataset_id", default=sisreg_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", default="oferta_programada")

    ####################################
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
    # Tasks section #2 - Transform data
    log(f"Raw file directory: {raw_file}")
    #oferta_programada_path = os.path.join(raw_file, "oferta_programada.csv")
    oferta_programada_path = f"{raw_file}oferta_programada.csv"
    log(f"Oferta Programada Path: {oferta_programada_path}")
    transformed_file = transform_data(file_path=oferta_programada_path, endpoint=ENDPOINT)

    #####################################
    # Tasks section #3 - Load data
    #####################################

    create_partitions_task = create_partitions(
        data_path=transformed_file,
        partition_directory=local_folders["partition_directory"],
        upstream_tasks=[transformed_file],
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
        upstream_tasks=[create_partitions_task],
    )


# Storage and run configs
sms_dump_sisreg.schedule = sisreg_daily_update_schedule
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
