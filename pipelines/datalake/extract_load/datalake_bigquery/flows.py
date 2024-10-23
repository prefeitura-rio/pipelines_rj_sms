# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SMSRio dumping flows
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.datalake_bigquery.tasks import clone_bigquery_table


with Flow(
    name="DataLake - Extração e Carga de Dados - Clonando Tabelas do Datalake",
) as datalake_bigquery_clone:
    ENVIRONMENT = Parameter("environment", default=False)
    TABLE_ID = Parameter("table_id", default=False)
    DBT_SELECT_EXP = Parameter("dbt_select_exp", default=None)

    bigquery_project = get_bigquery_project_name(
        environment=ENVIRONMENT
    )

    clone_table = clone_bigquery_table(
        table_id=TABLE_ID,
        project_name=bigquery_project,
    )

datalake_bigquery_clone.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datalake_bigquery_clone.executor = LocalDaskExecutor(num_workers=10)
datalake_bigquery_clone.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
