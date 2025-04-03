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
from pipelines.datalake.extract_load.historico_clinico_integrado.schedules import (
    hci_daily_update_schedule,
)
from pipelines.datalake.extract_load.historico_clinico_integrado.tasks import (
    build_gcp_table,
    download_from_db,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - HCI Aplicação",
) as dump_hci:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)
    HISTORICAL_MODE = Parameter("historical_mode", default=False)
    START_TARGET_DATE = Parameter("start_target_date", default="")
    END_TARGET_DATE = Parameter("end_target_date", default="")
    DATASET_ID = Parameter("dataset_id", default=hci_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)
    REFERENCE_DATETIME_COLUMN = Parameter("reference_datetime_column", default="created_at")

    # INFISICAL
    INFISICAL_PATH = hci_constants.INFISICAL_PATH.value
    INFISICAL_DBURL = hci_constants.INFISICAL_DB_URL.value

    #####################################
    # Set environment
    ####################################
    build_gcp_table_task = build_gcp_table(db_table=TABLE_ID)
    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT, table=TABLE_ID)

    ####################################
    # Tasks section #1 - Get data
    #####################################
    get_secret_task = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=INFISICAL_DBURL,
        environment="prod",
    )

    table = download_from_db(
        db_url=get_secret_task,
        db_table=TABLE_ID,
        start_target_date=START_TARGET_DATE,
        end_target_date=END_TARGET_DATE,
        historical_mode=HISTORICAL_MODE,
        reference_datetime_column=REFERENCE_DATETIME_COLUMN,
    )
    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    upload_df_to_datalake(
        df=table,
        dataset_id=DATASET_ID,
        table_id=build_gcp_table_task,
        source_format="parquet",
        partition_column="datalake_loaded_at",
    )


dump_hci.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
dump_hci.executor = LocalDaskExecutor(num_workers=1)
dump_hci.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="8Gi",
)
dump_hci.schedule = hci_daily_update_schedule
