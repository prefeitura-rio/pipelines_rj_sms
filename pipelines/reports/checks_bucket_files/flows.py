# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.reports.checks_bucket_files.schedules import schedule
from pipelines.reports.checks_bucket_files.tasks import (
    get_data_from_gcs_bucket,
    send_report,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="Report: Monitoramento de arquivos recorrentes",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.HERIAN_ID.value,
    ],
) as report_bucket_files:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment")
    CONFIGURATIONS = Parameter(
        "configurations",
        default=[
            {"bucket_name": "cgcca_cnes", "source_freshness": "M-0", "title": "Base de Dados CNES"}
        ],
    )

    #####################################
    # Tasks
    #####################################

    results = get_data_from_gcs_bucket.map(
        configuration=CONFIGURATIONS,
        environment=unmapped(ENVIRONMENT),
    )

    send_report(
        configurations=CONFIGURATIONS,
        results=results,
        environment=unmapped(ENVIRONMENT),
    )


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
