# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Tasks for execute_dbt
"""

import os
import shutil

import git
import prefect
from dbt.cli.main import dbtRunner, dbtRunnerResult
from prefect.client import Client
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.transform.dbt.constants import (
    constants as execute_dbt_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.dbt_log_processor import log_to_file, process_dbt_logs
from pipelines.utils.monitor import send_message


@task
def download_repository():
    """
    Downloads the repository specified by the REPOSITORY_URL constant.

    This function creates a repository folder, clones the repository from the specified URL,
    and logs the success or failure of the download.

    Raises:
        FAIL: If there is an error when creating the repository folder or downloading the repository.
    """
    repo_url = execute_dbt_constants.REPOSITORY_URL.value

    # create repository folder
    try:
        repository_path = os.path.join(os.getcwd(), "dbt_repository")

        if os.path.exists(repository_path):
            shutil.rmtree(repository_path, ignore_errors=False)
        os.makedirs(repository_path)

        log(f"Repository folder created: {repository_path}")

    except Exception as e:
        raise FAIL(str(f"Error when creating repository folder: {e}")) from e

    # download repository
    try:
        git.Repo.clone_from(repo_url, repository_path)
        log(f"Repository downloaded: {repo_url}")
    except git.GitCommandError as e:
        raise FAIL(str(f"Error when downloading repository: {e}")) from e

    return repository_path


@task
def execute_dbt(repository_path: str, command: str = "run", target: str = "dev", model: str = ""):
    """
    Execute commands in DBT.

    Args:
        command (str): Command to be executed by DBT. Can be "run", "build" or "test".
        model (str): Name of model. Can be empty.
        target (str): Name of the target that will be used by dbt
    """
    cli_args = [
        command,
        "--profiles-dir",
        repository_path,
        "--project-dir",
        repository_path,
        "--target",
        target,
    ]

    if model:
        cli_args.extend(["--models", model])

    dbt = dbtRunner()
    running_result: dbtRunnerResult = dbt.invoke(cli_args)

    return running_result


@task
def create_dbt_report(running_results: dbtRunnerResult) -> None:
    """
    Creates a report based on the results of running dbt commands.

    Args:
        running_results (dbtRunnerResult): The results of running dbt commands.

    Raises:
        FAIL: If there are failures in the dbt commands.

    Returns:
        None
    """
    logs = process_dbt_logs(log_path="dbt_repository/logs/dbt.log")
    log_path = log_to_file(logs)

    is_incomplete = False
    general_report = []
    for command_result in running_results.result:
        status = command_result.status
        if status == "fail":
            is_incomplete = True
            general_report.append(
                f"- 🛑 FAIL: `{command_result.node.name}`\n  - {command_result.message}"
            )  # noqa
        elif status == "error":
            is_incomplete = True
            general_report.append(
                f"- ❌ ERROR: `{command_result.node.name}`\n  - {command_result.message}"
            )  # noqa
        elif status == "warn":
            general_report.append(
                f"- ⚠️ WARN: `{command_result.node.name}`\n  - {command_result.message}"
            )  # noqa

    general_report = sorted(general_report)
    general_report = "**Resumo**:\n" + "\n".join(general_report)
    log(general_report)

    # Get Parameters
    param_report = ["**Parametros**:"]
    for key, value in prefect.context.get("parameters").items():
        if key == "rename_flow":
            continue
        param_report.append(f"- {key}: {value}")
    param_report = "\n".join(param_report)

    should_fail_execution = is_incomplete or not running_results.success

    # DBT - Sending Logs to Discord
    command = prefect.context.get("parameters").get("command")
    emoji = "❌" if should_fail_execution else "✅"
    complement = "com Erros" if should_fail_execution else "sem Erros"
    message = f"{param_report}\n{general_report}" if should_fail_execution else param_report

    send_message(
        title=f"{emoji} Execução `dbt {command}` finalizada {complement}",
        message=message,
        file_path=log_path,
        monitor_slug="dbt-runs",
    )

    if should_fail_execution:
        raise FAIL(general_report)


@task
def rename_current_flow_run_dbt(command: str, model: str, target: str) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()

    if model:
        client.set_flow_run_name(flow_run_id, f"dbt {command} --model {model} --target {target}")
    else:
        client.set_flow_run_name(flow_run_id, f"dbt {command} --target {target}")
