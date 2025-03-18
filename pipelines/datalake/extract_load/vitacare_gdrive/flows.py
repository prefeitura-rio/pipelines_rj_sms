# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
"""
SIH dumping flows
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.utils.data_extraction.google_drive import (
    explore_folder,
    dowload_from_gdrive,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import upload_df_to_datalake
from pipelines.utils.basics import from_relative_date
from pipelines.datalake.extract_load.vitacare_gdrive.constants import constants as gdrive_constants
from pipelines.datalake.extract_load.vitacare_gdrive.tasks import (
    get_folder_id,
    download_to_gcs,
)


with Flow(
    name="DataLake - Extração e Carga de Dados - Vitacare Reports GDrive",
) as sms_dump_vitacare_gdrive:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    AP = Parameter("ap", default="AP10", required=True)
    LAST_MODIFIED_DATE = Parameter("last_modified_date", default="M-1", required=False)

    #####################################
    # Tasks
    #####################################
    folder_id = get_folder_id(ap=AP)

    date_filter = from_relative_date(relative_date=LAST_MODIFIED_DATE)

    files = explore_folder(
        folder_id=folder_id,
        last_modified_date=date_filter
    )

    download_to_gcs.map(
        file_info=files,
        ap=unmapped(AP),
        environment=unmapped(ENVIRONMENT)
    )

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
