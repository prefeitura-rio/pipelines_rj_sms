# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tools.delete_flow_group.tasks import delete_flow_group


with Flow("Tool: Deletar Flow Group") as flow:
    ENVIRONMENT = Parameter("environment", default="staging")
    FLOW_GROUP_ID = Parameter("flow_group_id", default="")
    
    delete_flow_group(flow_group_id=FLOW_GROUP_ID)

flow.executor = LocalDaskExecutor(num_workers=10)
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
