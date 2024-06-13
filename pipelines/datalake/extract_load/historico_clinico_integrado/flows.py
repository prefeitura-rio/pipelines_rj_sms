# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123, C0301
# flake8: noqa E501
"""
Uploading patient data to datalake
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.historico_clinico_integrado.constants import (
    constants as hci_constants,
)

# from pipelines.datalake.extract_load.historico_clinico_integrado.schedules import (
#     vitacare_daily_update_schedule,
# )
from pipelines.datalake.extract_load.historico_clinico_integrado.tasks import (
    build_gcp_table,
    download_from_db,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import create_folders, get_secret_key, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - Histórico Clínico Integrado",
) as dump_hci:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)
    REPROCESS_MODE = Parameter("reprocess_mode", default=False)

    # INFISICAL
    INFISICAL_PATH = hci_constants.INFISICAL_PATH.value
    INFISICAL_DBURL = hci_constants.INFISICAL_DB_URL.value

    # GCP
    DATASET_ID = Parameter("dataset_id", default=hci_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    build_gcp_table_task = build_gcp_table(db_table=TABLE_ID)
    with case(RENAME_FLOW, True):

        rename_current_flow_run(environment=ENVIRONMENT)
    ####################################
    # Tasks section #1 - Get data
    #####################################
    get_secret_task = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_DBURL,
        environment=ENVIRONMENT,
    )

    create_folders_task = create_folders()

    download_task = download_from_db(
        db_url=get_secret_task,
        db_schema=hci_constants.SCHEMA_ID.value,
        db_table=TABLE_ID,
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID,
    )
    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    upload_to_datalake_task = upload_to_datalake(
        input_path=download_task,
        dataset_id=DATASET_ID,
        table_id=build_gcp_table_task,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
    )


dump_hci.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_hci.executor = LocalDaskExecutor(num_workers=1)
dump_hci.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
