# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Tasks for execute_dbt
"""

import json
import os
import shutil

import git
import prefect
from dbt.cli.main import (  # pylint: disable=import-error, no-name-in-module
    dbtRunner,
    dbtRunnerResult,
)
from prefect.client import Client
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.transform.dbt.constants import (
    constants as execute_dbt_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.dbt.dbt import Summarizer, log_to_file, process_dbt_logs
from pipelines.utils.dbt.extensions import classify_access
from pipelines.utils.googleutils import (
    download_from_cloud_storage,
    label_bigquery_table,
    tag_bigquery_table,
    upload_to_cloud_storage,
)
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
    select="",
    exclude="",
    flag="",
    **kwargs,
):
    """
    Executes a dbt command with the specified parameters.

    Args:
        repository_path (str): The path to the dbt repository.
        command (str, optional): The dbt command to execute. Defaults to "run".
        target (str, optional): The dbt target to use. Defaults to "dev".
        select (str, optional): The dbt selector to filter models. Defaults to "".
        exclude (str, optional): The dbt selector to exclude models. Defaults to "".

    Returns:
        dbtRunnerResult: The result of the dbt command execution.
    """
    commands = command.split(" ")

    cli_args = commands + ["--profiles-dir", repository_path, "--project-dir", repository_path]

    if command in ("build", "data_test", "run", "test", "ls"):

        cli_args.extend(
            [
                "--target",
                target,
            ]
        )

        if select:
            cli_args.extend(["--select", select])
        if exclude:
            cli_args.extend(["--exclude", exclude])
        if flag:
            cli_args.extend([flag])

        for key, value in kwargs.items():
            if value:
                cli_args.extend([f"--{key.replace('_', '-')}", value])

        log(f"Executing dbt command: {' '.join(cli_args)}", level="info")

    dbt_runner = dbtRunner()
    running_result: dbtRunnerResult = dbt_runner.invoke(cli_args)

    log_path = os.path.join(repository_path, "logs", "dbt.log")

    if command not in ("deps") and not os.path.exists(log_path):
        send_message(
            title="❌ Erro ao executar DBT",
            message="Não foi possível encontrar o arquivo de logs.",
            monitor_slug="dbt-runs",
        )
        raise FAIL("DBT Run seems not successful. No logs found.")

    return running_result


@task
def create_dbt_report(running_results: dbtRunnerResult, repository_path: str) -> None:
    """
    Creates a report based on the results of running dbt commands.

    Args:
        running_results (dbtRunnerResult): The results of running dbt commands.
        repository_path (str): The path to the repository.

    Raises:
        FAIL: If there are failures in the dbt commands.

    Returns:
        None
    """

    logs = process_dbt_logs(log_path=os.path.join(repository_path, "logs", "dbt.log"))
    log_path = log_to_file(logs)
    summarizer = Summarizer()

    is_successful, has_warnings = True, False

    general_report = []
    for command_result in running_results.result:
        if command_result.status == "fail":
            is_successful = False
            general_report.append(f"- 🛑 FAIL: {summarizer(command_result)}")
        elif command_result.status == "error":
            is_successful = False
            general_report.append(f"- ❌ ERROR: {summarizer(command_result)}")
        elif command_result.status == "warn":
            has_warnings = True
            general_report.append(f"- ⚠️ WARN: {summarizer(command_result)}")

    # Sort and log the general report
    general_report = sorted(general_report, reverse=True)
    general_report = "**Resumo**:\n" + "\n".join(general_report)
    log(general_report)

    # Get Parameters
    param_report = ["**Parametros**:"]
    for key, value in prefect.context.get("parameters").items():
        if key == "rename_flow":
            continue
        if value:
            param_report.append(f"- {key}: `{value}`")
    param_report = "\n".join(param_report)
    param_report += " \n"

    fully_successful = is_successful and running_results.success
    include_report = has_warnings or (not fully_successful)

    # DBT - Sending Logs to Discord
    command = prefect.context.get("parameters").get("command")
    emoji = "❌" if not fully_successful else "✅"
    complement = "com Erros" if not fully_successful else "sem Erros"
    message = f"{param_report}\n{general_report}" if include_report else param_report

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
    select: str,
    exclude: str,
) -> None:
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()

    flow_run_name = f"dbt {command}"

    if select:
        flow_run_name += f" --select {select}"
    if exclude:
        flow_run_name += f" --exclude {exclude}"

    flow_run_name += f" --target {target}"

    client.set_flow_run_name(flow_run_id, flow_run_name)
    log(f"Flow run renamed to: {flow_run_name}", level="info")


@task()
def get_target_from_environment(environment: str):
    """
    Retrieves the target environment based on the given environment parameter.

    Args:
        environment (str): The environment for which to retrieve the target.

    Returns:
        str: The target environment corresponding to the given environment.

    """
    converter = {
        "prod": "prod",
        "local-prod": "prod",
        "staging": "dev",
        "local-staging": "dev",
        "dev": "dev",
    }
    return converter.get(environment, "dev")


@task
def download_dbt_artifacts_from_gcs(dbt_path: str, environment: str):
    """
    Retrieves the dbt artifacts from Google Cloud Storage.
    """

    gcs_bucket = execute_dbt_constants.GCS_BUCKET.value[environment]

    target_base_path = os.path.join(dbt_path, "target_base")

    if os.path.exists(target_base_path):
        shutil.rmtree(target_base_path, ignore_errors=False)
        os.makedirs(target_base_path)

    try:
        download_from_cloud_storage(target_base_path, gcs_bucket)
        log(f"DBT artifacts downloaded from GCS bucket: {gcs_bucket}", level="info")
        return target_base_path

    except Exception as e:  # pylint: disable=broad-except
        log(f"Error when downloading DBT artifacts from GCS: {e}", level="error")
        return None


@task
def upload_dbt_artifacts_to_gcs(dbt_path: str, environment: str):
    """
    Sends the dbt artifacts to Google Cloud Storage.
    """

    dbt_artifacts_path = os.path.join(dbt_path, "target")

    gcs_bucket = execute_dbt_constants.GCS_BUCKET.value[environment]

    upload_to_cloud_storage(dbt_artifacts_path, gcs_bucket)
    log(f"DBT artifacts sent to GCS bucket: {gcs_bucket}", level="info")


@task
def check_if_dbt_artifacts_upload_is_needed(command: str):
    """
    Checks if the upload of dbt artifacts is needed.
    """

    if command in ["build", "source freshness"]:
        return True


@task
def add_access_tag_to_bq_tables(
    dbt_repository_path: str,
    dbt_state_file_path: str,
    dbt_select: str = None,
    dbt_exclude: str = None,
):
    """
    Tags the modified tables.
    """

    log("Identifying tables for tagging", level="info")
    # retrieve which tables need to be tagged
    tables = execute_dbt.run(
        repository_path=dbt_repository_path,
        command="ls",
        state=dbt_state_file_path,
        select=dbt_select,
        exclude=dbt_exclude,
        resource_type="model source",
    )

    log("Starting table tagging process", level="info")

    with open(f"{dbt_repository_path}/target/manifest.json", "r", encoding="utf-8") as f:
        manifest = json.load(f)

    for table in tables.result:

        # Retrieve table properties
        if "source" in table:
            dbt_type = "source"
            node = "sources"
            dbt_table_id = table.replace("source:", "source.")
        else:
            dbt_type = "model"
            node = "nodes"
            dbt_table_id = f"model.rj_sms.{table.split('.')[-1]}"

        properties = manifest[node][dbt_table_id]

        # Set BigQuery table name
        project = properties["database"]
        dataset = properties["schema"]
        table = properties["alias"] if dbt_type == "model" else properties["name"]
        classificacao = classify_access(properties)

        if classificacao in ["publico", "interno", "confidencial", "restrito"]:
            tag_bigquery_table(
                project_id=project,
                dataset_id=dataset,
                table_id=table,
                tag_key="classificacao",
                tag_value=classificacao,
            )

        label_bigquery_table(
            project_id=project,
            dataset_id=dataset,
            table_id=table,
            label_key="acesso",
            label_value=classificacao,
        )

    return
