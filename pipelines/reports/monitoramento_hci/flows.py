# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.reports.monitoramento_hci.schedules import schedule
from pipelines.reports.monitoramento_hci.tasks import get_data, send_report
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="Report: Monitoramento do HCI",
    state_handlers=[handle_flow_state_change],
    owners=[constants.AVELLAR_ID.value],
) as report_uso_hci:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET = Parameter("dataset_name", default="app_historico_clinico")
    TABLE = Parameter("table_name", default="registros")

    #####################################
    # Tasks
    #####################################
    data = get_data(dataset_name=DATASET, table_name=TABLE, environment=ENVIRONMENT)
    send_report(data=data, environment=ENVIRONMENT)


report_uso_hci.schedule = schedule
report_uso_hci.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_uso_hci.executor = LocalDaskExecutor(num_workers=1)
report_uso_hci.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
