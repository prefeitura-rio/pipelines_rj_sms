# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, flatten, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_rj.constants import (
    constants as flow_constants,
)

# from pipelines.datalake.extract_load.diario_oficial_rj.schedules import daily_schedule
from pipelines.datalake.extract_load.diario_oficial_rj.tasks import (
    get_article_contents,
    get_article_names_ids,
    get_current_DO_identifiers,
    upload_results,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="DataLake - Extração e Carga de Dados - Diário Oficial Municipal",
    state_handlers=[handle_flow_state_change],
    owners=[constants.AVELLAR_ID.value],
) as extract_diario_oficial_rj:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    DATE = Parameter("date", default=None)
    DATASET_ID = Parameter("dataset_id", default=flow_constants.DATASET_ID.value)

    # Podemos ter múltiplos DOs em um dia...
    diario_ids = get_current_DO_identifiers(date=DATE, env=ENVIRONMENT)

    # Para cada DO, pegamos todos os artigos...
    do_article_tuple = get_article_names_ids.map(diario_id_date=diario_ids)

    # Para cada par de nome/id, pega o conteúdo do artigo
    article_contents = get_article_contents.map(do_tuple=flatten(do_article_tuple))

    upload_results(results_list=article_contents, dataset=DATASET_ID, date=DATE, env=ENVIRONMENT)


# Por ora não vamos usar schedule; esse flow será chamado por um orquestrador
# extract_diario_oficial_rj.schedule = daily_schedule
extract_diario_oficial_rj.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_diario_oficial_rj.executor = LocalDaskExecutor(num_workers=1)
extract_diario_oficial_rj.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
    memory_request="5Gi",
)
