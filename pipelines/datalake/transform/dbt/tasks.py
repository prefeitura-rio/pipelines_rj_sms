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
def execute_dbt(
    repository_path: str,
    command: str = "run",
    target: str = "dev",
    model="",
    select="",
    exclude="",
):
    """
    Executes a dbt command with the specified parameters.

    Args:
        repository_path (str): The path to the dbt repository.
        command (str, optional): The dbt command to execute. Defaults to "run".
        target (str, optional): The dbt target to use. Defaults to "dev".
        model (str, optional): The specific dbt model to run. Defaults to "".
        select (str, optional): The dbt selector to filter models. Defaults to "".
        exclude (str, optional): The dbt selector to exclude models. Defaults to "".

    Returns:
        dbtRunnerResult: The result of the dbt command execution.
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
    if select:
        cli_args.extend(["--select", select])
    if exclude:
        cli_args.extend(["--exclude", exclude])

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

    is_successful, has_warnings = True, False

    general_report = []
    for command_result in running_results.result:
        status = command_result.status
        if status == "fail":
            is_successful = False
            general_report.append(
                f"- 🛑 FAIL: `{command_result.node.name}`\n   {command_result.message}: ``` select * from {command_result.node.relation_name.replace('`','')}``` \n"
            )
        elif status == "error":
            is_successful = False
            general_report.append(
                f"- ❌ ERROR: `{command_result.node.name}`\n  {command_result.message.replace('__','_')} \n"
            )
        elif status == "warn":
            has_warnings = True
            general_report.append(
                f"- ⚠️ WARN: `{command_result.node.name}`\n   {command_result.message}: ``` select * from {command_result.node.relation_name.replace('`','')}``` \n"
            )

    general_report = sorted(general_report, reverse=True)
    general_report = "**Resumo**:\n" + "\n".join(general_report)
    log(general_report)

    # Get Parameters
    param_report = ["**Parametros**:"]
    for key, value in prefect.context.get("parameters").items():
        if key == "rename_flow":
            continue
        if value:
            param_report.append(f"- {key}: {'`' + value + '`'}")
    param_report = "\n".join(param_report)
    param_report += " \n"

    fully_successful = is_successful and running_results.success
    include_report = has_warnings or (not fully_successful)

    # DBT - Sending Logs to Discord
    command = prefect.context.get("parameters").get("command")
    emoji = "❌" if not fully_successful else "✅"
    complement = "com Erros" if not fully_successful else "sem Erros"
    message = f"{param_report}\n{general_report}" if include_report else param_report

    if len(message) > 1600:
        while len(message) > 1600:
            last_item = message.rfind("- ")
            message = message[:last_item]
        message += " **Mensagem truncada devido ao tamanho. Verifique o log para mais detalhes.**"

    send_message(
        title=f"{emoji} Execução `dbt {command}` finalizada {complement}",
        message=message,
        file_path=log_path,
        monitor_slug="dbt-runs",
    )

    if not fully_successful:
        raise FAIL(general_report)


@task
def rename_current_flow_run_dbt(
    command: str,
    target: str,
    model: str,
    select: str,
    exclude: str,
) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()

    if model:
        client.set_flow_run_name(flow_run_id, f"dbt {command} --model {model} --target {target}")
    elif select:
        client.set_flow_run_name(flow_run_id, f"dbt {command} --select {select} --target {target}")
    elif exclude:
        client.set_flow_run_name(
            flow_run_id, f"dbt {command} --exclude {exclude} --target {target}"
        )
    else:
        client.set_flow_run_name(flow_run_id, f"dbt {command} --target {target}")
