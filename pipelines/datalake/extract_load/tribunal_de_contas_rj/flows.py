# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

from .constants import constants as flow_constants
from .tasks import (
    fetch_case_page,
    get_latest_vote,
    scrape_case_info_from_page,
    upload_results,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Tribunal de Contas do Município",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.AVELLAR_ID.value,
    ],
) as extract_tribunal_de_contas_rj:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    CASE_ID = Parameter("case_id", required=True)
    DATASET_ID = Parameter("dataset_id", default=flow_constants.DATASET_ID.value)

    # Caso principal
    case_obj, html = fetch_case_page(case_num=CASE_ID, env=ENVIRONMENT)
    result = scrape_case_info_from_page(case_tuple=(case_obj, html))
    # Tenta encontrar votos no caso
    latest_vote = get_latest_vote(ctid=case_obj["_ctid"])
    # Agrupamento de resultados e upload
    upload_results(main_result=result, latest_vote=latest_vote, dataset=DATASET_ID)


extract_tribunal_de_contas_rj.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_tribunal_de_contas_rj.executor = LocalDaskExecutor(num_workers=1)
extract_tribunal_de_contas_rj.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
    memory_request="1Gi",
)
