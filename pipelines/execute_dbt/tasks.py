# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Tasks for execute_dbt
"""

import os
import shutil

import git
import pandas as pd
import prefect
from dbt.cli.main import dbtRunner, dbtRunnerResult
from prefect.client import Client
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.execute_dbt.constants import constants as execute_dbt_constants
from pipelines.utils.credential_injector import authenticated_task as task
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

    # DBT Execution
    dbt = dbtRunner()
    res: dbtRunnerResult = dbt.invoke(cli_args)

    # DBT Log Extraction
    with open("dbt_repository/logs/dbt.log", "r", encoding="utf-8", errors="ignore") as log_file:
        logs = log_file.read()
        logs = pd.DataFrame({"text": logs.splitlines()})
        logs = logs[logs["text"].str.contains("\\[info")]

    report = "\n".join(logs["text"].to_list())
    log(f"Logs do DBT:{report}")

    with open("dbt_log.txt", "w+", encoding="utf-8") as log_file:
        log_file.write(report)

    has_failures = False
    failure_report = ["**Falhas**:"]
    for command_result in res.result:
        if command_result.status == "fail":
            has_failures = True
            failure_report.append(f"- `{command_result.node.name}`")
    failure_report = "\n".join(failure_report)

    # Get Parameters
    param_report = ["**Parametros**:"]
    for key, value in prefect.context.get("parameters").items():
        param_report.append(f"- {key}: {value}")
    param_report = "\n".join(param_report)

    # DBT - Sending Logs to Discord
    if has_failures or not res.success:
        send_message(
            title=f"❌ Execução `dbt {command}` finalizada com Erros",
            message=f"{param_report}\n{failure_report}",
            file_path="dbt_log.txt",
            monitor_slug="dbt-runs",
        )
        raise FAIL(failure_report)
    else:
        send_message(
            title=f"✅ Execução `dbt {command}` finalizada sem erros",
            message=f"{param_report}\nVerifique o log para mais detalhes.",
            file_path="dbt_log.txt",
            monitor_slug="dbt-runs",
        )


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
