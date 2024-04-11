# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123, C0301
# flake8: noqa E501
"""
Vitacare healthrecord dumping flows
"""
# from datetime import timedelta
from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_api.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_api.schedules import (
    vitacare_daily_update_schedule,
)
from pipelines.datalake.extract_load.sih_gdrive.tasks import (
    #create_partitions,
    transform_data,
)
from pipelines.datalake.extract_load.vitacare_api.tasks import (
    create_partitions,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.prontuarios.utils.tasks import (
    get_ap_from_cnes,
    get_current_flow_labels,
    get_healthcenter_name_from_cnes,
    get_project_name,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.tasks import create_folders, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - SIH",
) as sms_dump_sih:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)

    # SIH

    # GCP
    DATASET_ID = "brutos_sih"
    TABLE_ID =  "indicadores_hospitalares"

    #####################################
    # Tasks section #3 - Transform data
    #####################################

    transformed_file = transform_data(
        file_path="/Users/thiagotrabach/projects/pipelines_rj_sms/data/raw", env=ENVIRONMENT
    )

    #####################################
    # Tasks section #4 - Load data
    #####################################

    create_partitions_task = create_partitions(
        data_path="/Users/thiagotrabach/projects/pipelines_rj_sms/data/raw",
        partition_directory="/Users/thiagotrabach/projects/pipelines_rj_sms/data/partition_directory",
        file_type="parquet",
        upstream_tasks=[transformed_file],
    )

    upload_to_datalake_task = upload_to_datalake(
        input_path="/Users/thiagotrabach/projects/pipelines_rj_sms/data/partition_directory",
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
