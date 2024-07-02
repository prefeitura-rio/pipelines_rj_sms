# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tools.unschedule_old_flows.tasks import (
    archive_flow_versions,
    query_archived_flow_versions_with_runs,
    query_non_archived_flows,
    report_to_discord,
)

with Flow("Tool: Desagendador de Flows Fantasmas") as unscheduler_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)
    MODE = Parameter("mode", default="report", required=True)

    non_archived_flows = query_non_archived_flows(environment=ENVIRONMENT)

    archived_flow_versions_with_runs = query_archived_flow_versions_with_runs.map(
        flow_data=non_archived_flows, environment=unmapped(ENVIRONMENT)
    )

    with case(MODE, "fix"):
        cancel_archived_flow_runs = archive_flow_versions.map(
            archived_flow_runs=archived_flow_versions_with_runs
        )
    with case(MODE, "report"):
        report_to_discord(flows_to_archive=archived_flow_versions_with_runs)

unscheduler_flow.executor = LocalDaskExecutor(num_workers=1)
unscheduler_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
unscheduler_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
