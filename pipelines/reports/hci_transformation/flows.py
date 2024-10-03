# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.reports.hci_transformation.tasks import (
    calculate_data,
    get_diagram_template,
    generate_diagram,
)


with Flow(
    name="Report: Monitoramento de Ingestão e Transformações",
) as report_monitoramento_ingestao:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")
    ENTITY = Parameter("entity", default="paciente")
    TARGET_DATE = Parameter("target_date", required=True)

    #####################################
    # Tasks
    #####################################
    diagram_template = get_diagram_template(
        entity=ENTITY,
        environment=ENVIRONMENT
    )
    data = calculate_data(
        entity=ENTITY,
        environment=ENVIRONMENT
    )
    generate_diagram(
        diagram_template=diagram_template,
        data=data,
        target_date=TARGET_DATE,
    )


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
