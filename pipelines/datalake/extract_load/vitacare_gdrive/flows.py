# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
"""
SIH dumping flows
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.utils.data_extraction.google_drive import (
    get_files_from_folder,
    dowload_from_gdrive,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import upload_df_to_datalake
from pipelines.datalake.extract_load.vitacare_gdrive.constants import constants as gdrive_constants
from pipelines.datalake.extract_load.vitacare_gdrive.tasks import log_folder_structure


with Flow(
    name="DataLake - Extração e Carga de Dados - Vitacare Reports GDrive",
) as sms_dump_vitacare_gdrive:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    REFERENCE_MONTH = Parameter("reference_month", default="2025-02", required=True)

    #####################################
    # Tasks
    #####################################
    files = get_files_from_folder(
        folder_id=gdrive_constants.GDRIVE_FOLDER_ID.value,
        look_in_subfolders=True,
        file_extension=".csv",
    )

    log_folder_structure(files=files, environment=ENVIRONMENT)

# Storage and run configs
sms_dump_vitacare_gdrive.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_gdrive.executor = LocalDaskExecutor(num_workers=1)
sms_dump_vitacare_gdrive.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
