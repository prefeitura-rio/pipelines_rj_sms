# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.reports.long_running_flows.schedules import schedule
from pipelines.reports.long_running_flows.tasks import (
    cancel_flows,
    detect_running_flows,
    force_secrets_injection,
    report_flows,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import rename_current_flow_run

with Flow(
    "Report: Flows de Longa Execução",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.AVELLAR_ID.value,
    ],
) as report_long_running_flows:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    injected = force_secrets_injection(environment=ENVIRONMENT)

    rename_current_flow_run(environment=ENVIRONMENT)

    long_running_flows = detect_running_flows(environment=ENVIRONMENT, upstream_tasks=[injected])

    report_flows(running_flows=long_running_flows)

    cancel_flows(running_flows=long_running_flows)

report_long_running_flows.executor = LocalDaskExecutor(num_workers=1)
report_long_running_flows.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_long_running_flows.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
report_long_running_flows.schedule = schedule
