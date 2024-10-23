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
from pipelines.datalake.extract_load.datalake_bigquery.schedules import schedule
from pipelines.datalake.extract_load.datalake_bigquery.tasks import clone_bigquery_table
from pipelines.utils.basics import is_null_or_empty
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.tasks import (
    get_bigquery_project_from_environment,
    get_project_name,
    rename_current_flow_run,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Clonando Tabelas do Datalake",
) as datalake_bigquery_clone:
    ENVIRONMENT = Parameter("environment", default="dev")
    SOURCE_TABLE_ID = Parameter("source_table_id", default="")
    DESTINATION_TABLE_NAME = Parameter("destination_table_name", default="")
    DESTINATION_DATASET_NAME = Parameter("destination_dataset_name", default="")
    DBT_SELECT_EXP = Parameter("dbt_select_exp", default=None)

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    rename_current_flow_run(
        name_template="Cloning table {source_table_id} into {bigquery_project}",
        source_table_id=SOURCE_TABLE_ID,
        bigquery_project=bigquery_project,
    )

    clone_table_task = clone_bigquery_table(
        source_table_id=SOURCE_TABLE_ID,
        destination_table_name=DESTINATION_TABLE_NAME,
        destination_dataset_name=DESTINATION_DATASET_NAME,
        destination_project_name=bigquery_project,
    )

    with case(is_null_or_empty(value=DBT_SELECT_EXP), False):
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

datalake_bigquery_clone.schedule = schedule
datalake_bigquery_clone.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datalake_bigquery_clone.executor = LocalDaskExecutor(num_workers=1)
datalake_bigquery_clone.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
