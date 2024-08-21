# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
VitaCare dumping flows
"""
from prefect import Parameter, case
from prefect.run_configs import VertexRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_db.tasks import (
    load_local_data_to_raw,
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

    # VITACARE DB
    LOCAL_FILES = "/Users/thiagotrabach/tmp"

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()



    ####################################
    # Tasks section #1 - Extract data
    ####################################
    raw_file = load_local_data_to_raw(input_path=LOCAL_FILES, output_path=local_folders["raw"])

    #####################################
    # Tasks section #2 - Transform data
    #####################################

    transformed_file = transform_data(files_path=raw_file, env=ENVIRONMENT)

    #####################################
    # Tasks section #3 - Load data
    #####################################

    upload_to_datalake_task = upload_many_to_datalake(
        input_path=local_folders["raw"],
        dataset_id=DATASET_ID,
        source_format="parquet",
        if_exists="replace",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
    )
