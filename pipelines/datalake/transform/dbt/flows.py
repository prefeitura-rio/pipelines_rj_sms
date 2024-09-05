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
    check_if_dbt_artifacts_upload_is_needed,
    create_dbt_report,
    download_dbt_artifacts_from_gcs,
    download_repository,
    execute_dbt,
    get_target_from_environment,
    rename_current_flow_run_dbt,
    upload_dbt_artifacts_to_gcs,
    add_access_tag_to_bq_tables,
)

with Flow(name="DataLake - Transformação - DBT") as sms_execute_dbt:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)
    SEND_DISCORD_REPORT = Parameter("send_discord_report", default=True)

    # DBT
    COMMAND = Parameter("command", default="test", required=False)
    SELECT = Parameter("select", default=None, required=False)
    EXCLUDE = Parameter("exclude", default=None, required=False)
    FLAG = Parameter("flag", default=None, required=False)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")

    #####################################
    # Set environment
    ####################################
    target = get_target_from_environment(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run_dbt(
            command=COMMAND, select=SELECT, exclude=EXCLUDE, target=target
        )

    download_repository_task = download_repository()

    install_dbt_packages = execute_dbt(
        repository_path=download_repository_task,
        target=target,
        command="deps",
    )
    install_dbt_packages.set_upstream(download_repository_task)

    download_dbt_artifacts_task = download_dbt_artifacts_from_gcs(
        dbt_path=download_repository_task, environment=ENVIRONMENT
    )

    ####################################
    # Tasks section #1 - Execute commands in DBT
    #####################################

    running_results = execute_dbt(
        repository_path=download_repository_task,
        state=download_dbt_artifacts_task,
        target=target,
        command=COMMAND,
        select=SELECT,
        exclude=EXCLUDE,
        flag=FLAG,
    )
    running_results.set_upstream([install_dbt_packages, download_dbt_artifacts_task])

    with case(SEND_DISCORD_REPORT, True):
        create_dbt_report_task = create_dbt_report(
            running_results=running_results, repository_path=download_repository_task
        )

    ####################################
    # Task section #2 - Tag BigQuery Tables
    ################################

    # Classify tables
    # Tag tables
    add_access_tag_to_bq_tables_task = add_access_tag_to_bq_tables(
        dbt_repository_path=download_repository_task,
        dbt_state_file_path=download_dbt_artifacts_task,
        dbt_select=SELECT,
        dbt_exclude=EXCLUDE,
    )
    add_access_tag_to_bq_tables_task.set_upstream(running_results)

    ####################################
    # Tasks section #3 - Upload new artifacts to GCS
    #####################################

    check_if_upload_dbt_artifacts = check_if_dbt_artifacts_upload_is_needed(command=COMMAND)

    with case(check_if_upload_dbt_artifacts, True):
        upload_dbt_artifacts_to_gcs_task = upload_dbt_artifacts_to_gcs(
            dbt_path=download_repository_task, environment=ENVIRONMENT
        )
        upload_dbt_artifacts_to_gcs_task.set_upstream(
            [add_access_tag_to_bq_tables_task, running_results]
        )


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
