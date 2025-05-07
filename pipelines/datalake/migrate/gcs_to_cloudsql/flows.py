# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
#from pipelines.datalake.migrate.gcs_to_cloudsql.schedules import schedule
from pipelines.datalake.migrate.gcs_to_cloudsql.tasks import (
    get_most_recent_file_from_pattern,
    send_api_request,
)
from pipelines.utils.logger import log

with Flow(
    name="DataLake - Migração de Dados - GCS to Cloud SQL",
) as migrate_gcs_to_cloudsql:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    BUCKET_NAME = Parameter("bucket_name", default="vitacare_backups_gdrive")
    INSTANCE_NAME = Parameter("instance_name", default="vitacare")
    FILE_PATTERN = Parameter("file_pattern", default=None, required=True)
    # Testando com "HISTÓRICO_PEPVITA_RJ/AP10/vitacare_historic_*_*_*.bak"

    most_recent_file_name = get_most_recent_file_from_pattern(
        file_pattern=FILE_PATTERN,
        environment=ENVIRONMENT,
        bucket_name=BUCKET_NAME,
    )

    send_api_request(
        bucket_name=BUCKET_NAME,
        most_recent_file=most_recent_file_name,
        instance_name=INSTANCE_NAME
    )



# Storage and run configs
#migrate_gcs_to_cloudsql.schedule = schedule  # FIXME
migrate_gcs_to_cloudsql.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
migrate_gcs_to_cloudsql.executor = LocalDaskExecutor(num_workers=10)
migrate_gcs_to_cloudsql.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="10Gi",
)
