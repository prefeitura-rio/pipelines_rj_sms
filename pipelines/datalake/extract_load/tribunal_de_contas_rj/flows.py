# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.tribunal_de_contas_rj.constants import (
    constants as flow_constants,
)
from pipelines.datalake.extract_load.tribunal_de_contas_rj.tasks import (
    fetch_case_page,
    scrape_lastest_decision_from_page,
    upload_results,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Tribunal de Contas do Município",
) as extract_tribunal_de_contas_rj:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    CASE_ID = Parameter("case_id", required=True)
    DATASET_ID = Parameter("dataset_id", default=flow_constants.DATASET_ID.value)

    result_tuple = fetch_case_page(case_num=CASE_ID, env=ENVIRONMENT)

    (result, cases) = scrape_lastest_decision_from_page(case_tuple=result_tuple)

    # Podemos ter recebido apensos ao caso que precisamos pegar também
    appendix_tuple = fetch_case_page.map(case_num=cases, env=unmapped(ENVIRONMENT))
    results_list = scrape_lastest_decision_from_page.map(case_tuple=appendix_tuple)

    upload_results(main_result=result, appendix_results=results_list, dataset=DATASET_ID)


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
