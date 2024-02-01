# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tests.tasks import list_all_secrets_name
from pipelines.utils.tasks import inject_gcp_credentials

with Flow(
    name="Teste de Ambiente",
) as test_ambiente:

    ENVIRONMENT = Parameter("environment", default="dev")

    list_all_secrets_name(environment=ENVIRONMENT)

    inject_gcp_credentials(environment=ENVIRONMENT)

test_ambiente.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
test_ambiente.executor = LocalDaskExecutor(num_workers=10)
test_ambiente.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
