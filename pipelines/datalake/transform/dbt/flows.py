# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
DBT flows
"""
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.transform.dbt.schedules import dbt_schedules
from pipelines.datalake.transform.dbt.tasks import (
    create_dbt_report,
    download_repository,
    execute_dbt,
    rename_current_flow_run_dbt,
)

with Flow(name="DataLake - Transformação - DBT") as sms_execute_dbt:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # DBT
    COMMAND = Parameter("command", default="test", required=False)
    MODEL = Parameter("model", default=None, required=False)
    SELECT = Parameter("select", default=None, required=False)
    EXCLUDE = Parameter("exclude", default=None, required=False)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")

    #####################################
    # Set environment
    ####################################
    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run_dbt(
            command=COMMAND, model=MODEL, select=SELECT, exclude=EXCLUDE, target=ENVIRONMENT
        )

    ####################################
    # Tasks section #1 - Download repository and execute commands in DBT
    #####################################

    download_repository_task = download_repository()

    running_results = execute_dbt(
        repository_path=download_repository_task,
        command=COMMAND,
        target=ENVIRONMENT,
        model=MODEL,
        select=SELECT,
        exclude=EXCLUDE,
    )

    create_dbt_report_task = create_dbt_report(running_results=running_results)

# Storage and run configs
sms_execute_dbt.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_execute_dbt.executor = LocalDaskExecutor(num_workers=10)
sms_execute_dbt.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

sms_execute_dbt.schedule = dbt_schedules
