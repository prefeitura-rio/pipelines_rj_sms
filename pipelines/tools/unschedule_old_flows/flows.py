# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tools.unschedule_old_flows.tasks import (
    cancel_flows,
    get_prefect_client,
    query_active_flow_names,
    query_not_active_flows,
)

with Flow("Tool: Desagendador de Flows Antigos") as unscheduler_flow:
    ENVIRONMENT = Parameter("environment", default="staging")

    client = get_prefect_client()

    flows = query_active_flow_names(environment=ENVIRONMENT, prefect_client=client)

    archived_flow_runs = query_not_active_flows.map(
        flows=flows, environment=ENVIRONMENT, prefect_client=unmapped(client)
    )

    #cancel_flows.map(flows=archived_flow_runs, prefect_client=unmapped(client))

unscheduler_flow.executor = LocalDaskExecutor(num_workers=10)
unscheduler_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
unscheduler_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
