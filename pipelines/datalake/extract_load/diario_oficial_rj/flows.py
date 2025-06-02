# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.diario_oficial_rj.constants import (
    constants as flow_constants,
)
from pipelines.datalake.extract_load.diario_oficial_rj.schedules import schedule
from pipelines.datalake.extract_load.diario_oficial_rj.tasks import (
    get_current_DO_identifiers,
    get_article_names_ids,
    get_article_contents,
    upload_results
)


with Flow(
    name="DataLake - Extração e Carga de Dados - Diário Oficial Municipal",
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

    # Para cada DO, pegamos todos os decretos...
    names_and_ids = get_article_names_ids.map(
        diario_id=diario_ids,
        test=unmapped(True)
    )

    # FIXME: `article_contents` aqui é a função (???) e não o resultado
    article_contents = get_article_contents.map(
        obj=names_and_ids,
        test=unmapped(True)
    )

    upload_results(results_list=article_contents, dataset=DATASET_ID)



# Storage and run configs
extract_diario_oficial_rj.schedule = schedule
extract_diario_oficial_rj.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_diario_oficial_rj.executor = LocalDaskExecutor(num_workers=1)
extract_diario_oficial_rj.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="13Gi",
    memory_request="13Gi",
)
