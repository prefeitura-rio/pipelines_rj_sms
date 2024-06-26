# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tools.api_healthcheck.schedules import update_schedule
from pipelines.tools.api_healthcheck.tasks import (
    check_api_health,
    get_api_url,
    insert_results,
)
from pipelines.utils.tasks import load_file_from_bigquery

with Flow(
    name="Tool: Monitoramento de API",
) as monitoramento_api:
    ENVIRONMENT = Parameter("environment", default="dev")
    TAG = Parameter("tag", default="")

    endpoints = load_file_from_bigquery(
        project_name="rj-sms",
        dataset_name="gerenciamento__monitoramento_de_api",
        table_name="endpoint",
        environment=ENVIRONMENT,
    )

    api_url_list = get_api_url(
        api_url_table=endpoints,
        tag=TAG,
    )

    results = check_api_health.map(api_info=api_url_list)

    insert_results(rows_to_insert=results)

monitoramento_api.schedule = update_schedule
monitoramento_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
monitoramento_api.executor = LocalDaskExecutor(num_workers=10)
monitoramento_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
