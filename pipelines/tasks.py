
import os
import git
import shutil
from prefect import task
from dbt.cli.main import dbtRunner, dbtRunnerResult
from prefeitura_rio.pipelines_utils.logging import log
from prefect.engine.signals import FAIL


@task
def execute_dbt(
    command: str = 'run', 
    target: str = 'dev',
    model: str = ''
):
    """
    Download repository and execute commands in DBT.

    Args:
        command (str): Command to be executed by DBT. Can be "run" or "build".
        model (str): Name of model. Can be empty.
        target (str): Name of the target that will be used by dbt
    """
    # Repository download
    repo_url = "https://github.com/prefeitura-rio/queries-rj-sms.git"
    path = "pipelines/github/queries-rj-sms/"
    try:
        if os.path.exists(path):
            shutil.rmtree(path, ignore_errors=True)
        repo = git.Repo.clone_from(repo_url, path)
        log(f"Cloned repository in {path}")
        #Execute DBT
        dbt = dbtRunner()
        if not model:
            cli_args = [command, "--profiles-dir", path, "--project-dir", path, "--target", target]
        else:
            cli_args = [command, "--profiles-dir", path, "--project-dir", path, "--target", target, "--models", model]
        res: dbtRunnerResult = dbt.invoke(cli_args)
        try:
            for r in res.result:
                log(f"{r.node.name}: {r.status}")
                if r.status == "fail":
                    raise FAIL(str(f"DBT task {r.node.name} failed."))
        except Exception as e:
            log(f"An error occurred: {e}")
    except git.GitCommandError as e:
        log(f"Error when cloning repository: {e}")

