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
from pipelines.execute_dbt.schedules import dbt_daily_update_schedule
from pipelines.execute_dbt.tasks import (
    download_repository,
    execute_dbt,
    rename_current_flow_run_dbt,
)
from pipelines.utils.tasks import inject_gcp_credentials

with Flow(name="DBT - Executar comando no projeto queries-rj-sms") as sms_execute_dbt:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # DBT
    COMMAND = Parameter("command", default="test", required=False)
    MODEL = Parameter("model", default=None, required=False)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run_dbt(
            command=COMMAND, model=MODEL, target=ENVIRONMENT
        )
        rename_flow_task.set_upstream(inject_gcp_credentials_task)

    ####################################
    # Tasks section #1 - Download repository and execute commands in DBT
    #####################################

    download_repository_task = download_repository()
    download_repository_task.set_upstream(inject_gcp_credentials_task)

    execute_dbt_task = execute_dbt(
        repository_path=download_repository_task, command=COMMAND, target=ENVIRONMENT, model=MODEL
    )
    execute_dbt_task.set_upstream(download_repository_task)

# Storage and run configs
sms_execute_dbt.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_execute_dbt.executor = LocalDaskExecutor(num_workers=10)
sms_execute_dbt.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)
sms_execute_dbt.schedule = dbt_daily_update_schedule
