# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants

with Flow(
    name="Report: Ingestão de Dados",
) as flow:
    ENVIRONMENT = Parameter("environment", default="staging")

    

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.executor = LocalDaskExecutor(num_workers=1)
flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
