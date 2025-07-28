# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.tools.unschedule_old_flows.schedules import schedule
from pipelines.tools.unschedule_old_flows.tasks import (
    archive_flow_versions,
    query_archived_flow_versions_with_runs,
    query_non_archived_flows,
    report_to_discord,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    "Tool: Desagendador de Flows Fantasmas",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.AVELLAR_ID.value,
    ],
) as unscheduler_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)
    MODE = Parameter("mode", default="report", required=True)

    non_archived_flows = query_non_archived_flows(environment=ENVIRONMENT)

    archived_flow_versions_with_runs = query_archived_flow_versions_with_runs.map(
        flow_data=non_archived_flows, environment=unmapped(ENVIRONMENT)
    )

    with case(MODE, "fix"):
        cancel_archived_flow_runs = archive_flow_versions.map(
            flow_versions_to_archive=archived_flow_versions_with_runs
        )
    with case(MODE, "report"):
        report_to_discord(flows_to_archive=archived_flow_versions_with_runs)

unscheduler_flow.executor = LocalDaskExecutor(num_workers=1)
unscheduler_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
unscheduler_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
unscheduler_flow.schedule = schedule
