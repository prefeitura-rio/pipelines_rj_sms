# -*- coding: utf-8 -*-
"""
Fluxo
"""

# prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import constants

from pipelines.datalake.extract_load.centralregulacao_mysql_teste.schedules import schedule
from pipelines.datalake.extract_load.centralregulacao_mysql_teste.tasks import dummy

with Flow(name="SUBGERAL - Teste") as test_flow:
    ENVIRONMENT = Parameter("environment")
    NAME = Parameter("name")

    dummy(
        environment=ENVIRONMENT,
        name=NAME,
    )

test_flow.executor = LocalDaskExecutor(num_workers=1)
test_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
test_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="10Gi",
    memory_limit="10Gi",
)
test_flow.schedule = schedule
