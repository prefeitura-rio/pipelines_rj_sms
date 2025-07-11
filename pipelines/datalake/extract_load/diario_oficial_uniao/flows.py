# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_uniao.schedules import schedule
from pipelines.datalake.extract_load.diario_oficial_uniao.tasks import (
    dou_extraction,
    parse_date,
    upload_to_datalake,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Diário Oficial da União"
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

    date = parse_date(date_string=DATE)
    dou_infos = dou_extraction(date=date, dou_section=DOU_SECTION, max_workers=MAX_WORKERS)
    upload_to_datalake(dou_infos=dou_infos, environment=ENVIRONMENT, dataset=DATASET_ID)


# Flow configs
extract_diario_oficial_uniao.schedule = schedule
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
