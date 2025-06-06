# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.reports.checks_bucket_files.schedules import schedule
from pipelines.reports.checks_bucket_files.tasks import (
    get_data_from_gcs_bucket,
    send_report,
)
from pipelines.utils.time import from_relative_date

with Flow(name="Report: Monitoramento de arquivos recorrentes") as report_bucket_files:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment")
    BUCKET_NAME = Parameter("bucket_name")
    SOURCE_FRESHNESS = Parameter("source_freshness")

    #####################################
    # Tasks
    #####################################
    relative_date = from_relative_date(relative_date=SOURCE_FRESHNESS)

    files_count = get_data_from_gcs_bucket(
        bucket_name=BUCKET_NAME, relative_date=relative_date, environment=ENVIRONMENT
    )

    send_report(bucket_name=BUCKET_NAME, source_freshness=SOURCE_FRESHNESS, len_files=files_count)


report_bucket_files.schedule = schedule
report_bucket_files.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_bucket_files.executor = LocalDaskExecutor(num_workers=1)
report_bucket_files.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
