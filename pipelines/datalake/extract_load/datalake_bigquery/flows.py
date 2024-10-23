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
from pipelines.utils.basics import is_null_or_empty
from pipelines.utils.tasks import (
    rename_current_flow_run,
    get_bigquery_project_from_environment,
    get_project_name,
)
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.datalake.extract_load.datalake_bigquery.tasks import clone_bigquery_table


with Flow(
    name="DataLake - Extração e Carga de Dados - Clonando Tabelas do Datalake",
) as datalake_bigquery_clone:
    ENVIRONMENT = Parameter("environment", default=False)
    TABLE_ID = Parameter("table_id", default=False)
    DBT_SELECT_EXP = Parameter("dbt_select_exp", default=None)

    bigquery_project = get_bigquery_project_from_environment(
        environment=ENVIRONMENT
    )

    rename_current_flow_run(
        name_template="Cloning table {table_id} from {bigquery_project}",
        table_id=TABLE_ID,
        bigquery_project=bigquery_project,
    )

    clone_table_task = clone_bigquery_table(
        table_id=TABLE_ID,
        project_name=bigquery_project,
    )

    with case(is_null_or_empty(DBT_SELECT_EXP), False):
        current_flow_run_labels = get_current_flow_labels()
        project_name = get_project_name(environment=ENVIRONMENT)

        dbt_run_flow = create_flow_run(
            flow_name="DataLake - Transformação - DBT",
            project_name=project_name,
            parameters={
                "command": "run",
                "select": DBT_SELECT_EXP,
                "environment": ENVIRONMENT,
                "rename_flow": True,
                "send_discord_report": False,
            },
            labels=current_flow_run_labels,
            upstream_tasks=[clone_table_task],
        )
        wait_for_flow_run(flow_run_id=dbt_run_flow)


datalake_bigquery_clone.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datalake_bigquery_clone.executor = LocalDaskExecutor(num_workers=10)
datalake_bigquery_clone.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
