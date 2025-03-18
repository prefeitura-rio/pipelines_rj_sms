# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.migrate.vitacare_gdrive.tasks import (
    download_to_gcs,
    get_folder_id,
)
from pipelines.datalake.utils.data_extraction.google_drive import explore_folder
from pipelines.utils.basics import from_relative_date
from pipelines.utils.tasks import rename_current_flow_run

with Flow(
    name="DataLake - Migração de Dados - Vitacare GDrive",
) as sms_dump_vitacare_gdrive:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    AP = Parameter("ap", default="AP10", required=True)
    LAST_MODIFIED_DATE = Parameter("last_modified_date", default="M-1", required=False)
    RENAME_FLOW_RUN = Parameter("rename_flow_run", default=False, required=False)

    #####################################
    # Tasks
    #####################################

    with case(RENAME_FLOW_RUN, True):
        rename_current_flow_run(
            name_template="Migrando informes da {ap} | {start} | {environment}",
            ap=AP,
            start=LAST_MODIFIED_DATE,
            environment=ENVIRONMENT,
        )

    folder_id = get_folder_id(ap=AP)

    date_filter = from_relative_date(relative_date=LAST_MODIFIED_DATE)

    files = explore_folder(folder_id=folder_id, last_modified_date=date_filter)

    download_to_gcs.map(file_info=files, ap=unmapped(AP), environment=unmapped(ENVIRONMENT))

# Storage and run configs
sms_dump_vitacare_gdrive.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_gdrive.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare_gdrive.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
