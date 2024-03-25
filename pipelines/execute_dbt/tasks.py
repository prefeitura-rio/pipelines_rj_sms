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
import logging
from prefect import task
from prefect.client import Client
from prefect.utilities.logging import get_logger
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.execute_dbt.constants import constants as execute_dbt_constants


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
    prefect_logger = get_logger()

    dbt_logger = logging.getLogger('dbt')
    for handler in prefect_logger.handlers:
        dbt_logger.addHandler(handler)
    dbt_logger.setLevel(logging.DEBUG)

    try:
        # Execute DBT
        dbt = dbtRunner()
        if not model:
            cli_args = [
                command,
                "--profiles-dir",
                repository_path,
                "--project-dir",
                repository_path,
                "--target",
                target,
            ]
        else:
            cli_args = [
                command,
                "--profiles-dir",
                repository_path,
                "--project-dir",
                repository_path,
                "--target",
                target,
                "--models",
                model,
            ]
        res: dbtRunnerResult = dbt.invoke(cli_args)
        failures = []
        for command_result in res.result:
            log(f"Command: {command_result.node.name} - Status: {command_result.status}")
            if command_result.status == "fail":
                failures.append(command_result.node.name)
        if len(failures) > 0:
            raise FAIL(f"{len(failures)} tasks failed: {failures}")
    except git.GitCommandError as e:
        log(f"Error when cloning repository: {e}")


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
