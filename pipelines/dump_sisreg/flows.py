# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SISREG dumping flows
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_rename_current_flow_run_dataset_table,
)

from pipelines.constants import constants
#from pipelines.dump_url.schedules import daily_update_schedule
from pipelines.utils.tasks import (
    create_folders,
    download_from_url,
    inject_gcp_credentials,
    fix_columns_names,
    upload_to_datalake,
)

with Flow(
    name="Dump SISTEG - Ingerir dados de SISREG",
) as sms_dump_sisreg:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # SISREG
    FILE_PATH = "/Users/thiagotrabach/projects/pipelines_rj_sms/data/raw/sisreg_escalas.csv"

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = "brutos_sisreg"  # Parameter("dataset_id", required=True)
    TABLE_ID = "escalas"  # Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = task_rename_current_flow_run_dataset_table(
            prefix="Dump URL: ",
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            upstream_tasks=[inject_gcp_credentials_task],
        )

    ####################################
    # Tasks section #1 - Get data
    #####################################
    # create_folders_task = create_folders(upstream_tasks=[inject_gcp_credentials_task])

    # download_task =

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    fix_columns_names_task = fix_columns_names(
        file_path=FILE_PATH,
        csv_delimiter=";",
        encoding="latin-1",
        upstream_tasks=[inject_gcp_credentials_task],
    )

    upload_to_datalake_task = upload_to_datalake(
        input_path=FILE_PATH,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        if_exists="replace",
        csv_delimiter=";",
        dump_mode="overwrite",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[inject_gcp_credentials_task],
    )


# sms_dump_url.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# sms_dump_url.executor = LocalDaskExecutor(num_workers=10)
# sms_dump_url.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value,
#     labels=[
#         constants.RJ_SMS_AGENT_LABEL.value,
#     ],
#     memory_limit="2Gi",
# )
#
# sms_dump_url.schedule = daily_update_schedule
