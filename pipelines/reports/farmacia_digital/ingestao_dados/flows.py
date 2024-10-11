# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.reports.farmacia_digital.ingestao_dados.tasks import (
    get_data,
    send_report,
)

with Flow(
    name="Report: Farmácia Digital - Monitoramento de Ingestão de Dados",
) as report_farmacia_monitoramento_ingestao:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")
    TARGET_DATE = Parameter("target_date", default=None)

    #####################################
    # Tasks
    #####################################
    # rename_current_flow_run(environment=ENVIRONMENT, target_date=TARGET_DATE)

    data = get_data(environment=ENVIRONMENT, target_date=TARGET_DATE)

    send_report(data=data, target_date=TARGET_DATE)
    

report_farmacia_monitoramento_ingestao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_farmacia_monitoramento_ingestao.executor = LocalDaskExecutor(num_workers=1)
report_farmacia_monitoramento_ingestao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
