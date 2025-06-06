# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, flatten
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.tribunal_de_contas_rj.constants import (
    constants as flow_constants,
)
# from pipelines.datalake.extract_load.tribunal_de_contas_rj.schedules import schedule
from pipelines.datalake.extract_load.tribunal_de_contas_rj.tasks import (
    fetch_process_page
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Tribunal de Contas do Município",
) as extract_tribunal_de_contas_rj:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    PROCESS_ID = Parameter("process_id", required=True)
    DATASET_ID = Parameter("dataset_id", default=flow_constants.DATASET_ID.value)

    fetch_process_page(process_num=PROCESS_ID, env=ENVIRONMENT)

    #upload_results(results_list=article_contents, dataset=DATASET_ID)


# FIXME: schedule?
# extract_tribunal_de_contas_rj.schedule = schedule
extract_tribunal_de_contas_rj.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_tribunal_de_contas_rj.executor = LocalDaskExecutor(num_workers=1)
extract_tribunal_de_contas_rj.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="13Gi",
    memory_request="13Gi",
)
