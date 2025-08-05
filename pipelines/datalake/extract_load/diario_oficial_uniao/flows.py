# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_uniao.tasks import (
    dou_extraction,
    report_extraction_status,
    upload_to_datalake,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="DataLake - Extração e Carga de Dados - Diário Oficial da União",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as extract_diario_oficial_uniao:
    """
    Fluxo de extração e carga de atos oficiais do Diário Oficial da União (DOU).
    """

    #####################################
    # Parameters
    #####################################

    ENVIRONMENT = Parameter("environment", default="dev")
    DATE = Parameter("date", default="")
    DOU_SECTION = Parameter("dou_section", default=3)
    MAX_WORKERS = Parameter("max_workers", default=10)
    DATASET_ID = Parameter("dataset_id", default="brutos_diario_oficial")

    #####################################
    # Flow
    #####################################

    dou_infos, is_successful = dou_extraction(
        date=DATE, dou_section=DOU_SECTION, max_workers=MAX_WORKERS
    )
    upload_to_datalake(dou_infos=dou_infos, dataset=DATASET_ID)
    report_extraction_status(
        status=is_successful, date=DATE, dou_section=DOU_SECTION, environment=ENVIRONMENT
    )

# Flow configs
extract_diario_oficial_uniao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_diario_oficial_uniao.executor = LocalDaskExecutor(num_workers=10)
extract_diario_oficial_uniao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="13Gi",
    memory_request="13Gi",
)
