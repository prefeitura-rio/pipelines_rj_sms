# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.migrate.gdrive_to_gcs import schedules
from pipelines.datalake.migrate.gdrive_to_gcs.tasks import (
    download_to_gcs,
    get_fully_qualified_bucket_name,
)
from pipelines.utils.basics import from_relative_date
from pipelines.utils.flow import Flow
from pipelines.utils.google_drive import get_files_from_folder, get_folder_name
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import rename_current_flow_run

with Flow(
    name="DataLake - Migração de Dados - GDrive to GCS",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DIT_ID.value],
) as migrate_gdrive_to_gcs:
    #####################################
    # Parameters
    #####################################

    # Flow
    BUCKET_NAME = Parameter("bucket_name", default=None, required=True)
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    FOLDER_ID = Parameter("folder_id", default=None, required=True)
    OWNER_EMAIL = Parameter("owner_email", default=None, required=False)
    LAST_MODIFIED_DATE = Parameter("last_modified_date", default="M-1", required=False)
    RENAME_FLOW = Parameter("rename_flow", default=False, required=False)

    #####################################
    # Tasks
    #####################################
    folder_name = get_folder_name(folder_id=FOLDER_ID)

    fully_qualified_bucket_name = get_fully_qualified_bucket_name(
        bucket_name=BUCKET_NAME, environment=ENVIRONMENT
    )

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            name_template="Migrando drive://{folder_name}/ -> gs://{bucket_name}/{folder_name}/ | >={start} | {environment}",  # noqa: E501
            folder_name=folder_name,
            bucket_name=fully_qualified_bucket_name,
            start=LAST_MODIFIED_DATE,
            environment=ENVIRONMENT,
        )

    date_filter = from_relative_date(relative_date=LAST_MODIFIED_DATE)

    files = get_files_from_folder(
        folder_id=FOLDER_ID,
        last_modified_date=date_filter,
        owner_email=OWNER_EMAIL,
    )

    download_to_gcs.map(
        file_info=files,
        bucket_name=unmapped(fully_qualified_bucket_name),
        folder_name=unmapped(folder_name),
    )

# Storage and run configs
migrate_gdrive_to_gcs.schedule = schedules
migrate_gdrive_to_gcs.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
migrate_gdrive_to_gcs.executor = LocalDaskExecutor(num_workers=2)
migrate_gdrive_to_gcs.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="13Gi",
)
