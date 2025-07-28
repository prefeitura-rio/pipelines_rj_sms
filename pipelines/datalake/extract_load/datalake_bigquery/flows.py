# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
SMSRio dumping flows
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

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
from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_bigquery_project_from_environment,
    get_project_name,
    rename_current_flow_run,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Clonando Tabelas do Datalake",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DIT_ID.value,
    ],
) as datalake_bigquery_clone:

    ENVIRONMENT = Parameter("environment", default="dev")
    SLUG = Parameter("slug", default="")
    SOURCE_PROJECT_NAME = Parameter("source_project_name", default="rj-smfp")
    SOURCE_DATASET_NAME = Parameter("source_dataset_name", default="")
    SOURCE_TABLE_LIST = Parameter("source_table_list", default=[])
    DESTINATION_DATASET_NAME = Parameter("destination_dataset_name", default="")
    DBT_SELECT_EXP = Parameter("dbt_select_exp", default=None)

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    rename_current_flow_run(
        name_template="Cloning dataset {slug} into {bigquery_project}",
        slug=SLUG,
        bigquery_project=bigquery_project,
    )

    clone_table_task = clone_bigquery_table(
        source_project_name=SOURCE_PROJECT_NAME,
        source_dataset_name=SOURCE_DATASET_NAME,
        source_table_list=SOURCE_TABLE_LIST,
        destination_project_name=bigquery_project,
        destination_dataset_name=DESTINATION_DATASET_NAME,
        upstream_tasks=[rename_current_flow_run],
    )

    received_dbt_select = is_null_or_empty(value=DBT_SELECT_EXP, upstream_tasks=[clone_table_task])

    with case(received_dbt_select, False):
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
