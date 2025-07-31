# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.reports.ingestao_dados.schedules import schedule
from pipelines.reports.ingestao_dados.tasks import get_base_date, get_data, send_report
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="Report: Monitoramento de Ingestão de Dados",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DIT_ID.value,
    ],
) as report_monitoramento_ingestao:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")
    BASE_DATE = Parameter("base_date", default="yesterday")

    #####################################
    # Tasks
    #####################################
    data = get_data(environment=ENVIRONMENT)

    base_date = get_base_date(base_date=BASE_DATE)

    send_report(data=data, base_date=base_date)


report_monitoramento_ingestao.schedule = schedule
report_monitoramento_ingestao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_monitoramento_ingestao.executor = LocalDaskExecutor(num_workers=1)
report_monitoramento_ingestao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
