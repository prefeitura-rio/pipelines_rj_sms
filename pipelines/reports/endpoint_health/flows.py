# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.reports.endpoint_health.schedules import update_schedule
from pipelines.reports.endpoint_health.tasks import (
    create_and_send_report,
    filter_dataframe_using_tag,
)
from pipelines.utils.tasks import load_file_from_bigquery

with Flow(
    name="Report: Disponibilidade de API",
) as disponibilidade_api:
    ENVIRONMENT = Parameter("environment", default="staging")
    FILTER_TAG = Parameter("filter_tag", default="")

    all_endpoints_table = load_file_from_bigquery(
        project_name="rj-sms",
        dataset_name="gerenciamento__monitoramento_de_api",
        table_name="endpoint",
        environment=ENVIRONMENT,
    )
    endpoints_table_with_tag = filter_dataframe_using_tag(
        dataframe=all_endpoints_table, tag=FILTER_TAG
    )
    health_check_results_table = load_file_from_bigquery(
        project_name="rj-sms",
        dataset_name="gerenciamento__monitoramento_de_api",
        table_name="endpoint_ping",
        environment=ENVIRONMENT,
    )
    create_and_send_report(
        endpoints_table=endpoints_table_with_tag,
        results_table=health_check_results_table,
        filter_tag=FILTER_TAG,
    )

disponibilidade_api.schedule = update_schedule
disponibilidade_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
disponibilidade_api.executor = LocalDaskExecutor(num_workers=1)
disponibilidade_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
