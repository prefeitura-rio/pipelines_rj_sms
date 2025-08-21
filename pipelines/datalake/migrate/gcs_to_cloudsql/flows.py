# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.migrate.gcs_to_cloudsql.schedules import schedule
from pipelines.datalake.migrate.gcs_to_cloudsql.tasks import (
    check_for_outdated_backups,
    find_all_filenames_from_pattern,
    get_most_recent_filenames,
    limit_files_task,
    process_single_database_backup,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="DataLake - Migração de Dados - GCS to Cloud SQL",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DIT_ID.value],
) as migrate_gcs_to_cloudsql:
    #####################################
    # Parameters
    #####################################

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    BUCKET_NAME = Parameter("bucket_name", default="vitacare_backups_gdrive")
    INSTANCE_NAME = Parameter("instance_name", default="vitacare")
    FILE_PATTERN = Parameter("file_pattern", default=None, required=True)
    LIMIT_FILES = Parameter("limit_files", default=None)

    filenames = find_all_filenames_from_pattern(
        environment=ENVIRONMENT,
        file_pattern=FILE_PATTERN,
        bucket_name=BUCKET_NAME,
    )

    most_recent_filenames = get_most_recent_filenames(files=filenames)

    check_for_outdated_backups(
        most_recent_filenames=most_recent_filenames,
        upstream_tasks=[most_recent_filenames],
    )

    limited_filenames = limit_files_task(
        files=most_recent_filenames,
        limit=LIMIT_FILES,
    )

    process_task = process_single_database_backup.map(
        file=limited_filenames,
        bucket_name=unmapped(BUCKET_NAME),
        instance_name=unmapped(INSTANCE_NAME),
    )


# Storage and run configs
migrate_gcs_to_cloudsql.schedule = schedule
migrate_gcs_to_cloudsql.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
migrate_gcs_to_cloudsql.executor = LocalDaskExecutor(num_workers=1)
migrate_gcs_to_cloudsql.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="10Gi",
)