# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tools.unschedule_old_flows.tasks import (
    query_archived_flow_versions_with_runs,
    query_non_archived_flows,
)

with Flow("Tool: Desagendador de Flows Fantasmas") as unscheduler_flow:
    ENVIRONMENT = Parameter("environment", default="staging")

    non_archived_flows = query_non_archived_flows(environment=ENVIRONMENT)

    archived_flow_runs = query_archived_flow_versions_with_runs.map(
        flow_data=non_archived_flows,
        environment=ENVIRONMENT
    )

unscheduler_flow.executor = LocalDaskExecutor(num_workers=1)
unscheduler_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
unscheduler_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
