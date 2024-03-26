# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.reports.data_ingestion.schedules import update_schedule
from pipelines.constants import constants
from pipelines.reports.data_ingestion.tasks import (
    create_report,
    get_inserted_registers,
    get_prontuarios_database_url,
    get_target_date,
)

with Flow(
    name="Report: Ingestão de Dados",
) as flow:
    ENVIRONMENT = Parameter("environment", default="staging")
    CUSTOM_TARGET_DATE = Parameter("custom_target_date", default="")

    db_url = get_prontuarios_database_url(environment=ENVIRONMENT)
    target_date = get_target_date(custom_target_date=CUSTOM_TARGET_DATE)
    data = get_inserted_registers(
        target_date=target_date,
        db_url=db_url,
    )
    create_report(target_date=target_date, data=data)

flow.schedule = update_schedule
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
