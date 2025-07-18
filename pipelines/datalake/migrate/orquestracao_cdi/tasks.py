# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from typing import Optional

from pipelines.utils.credential_injector import authenticated_task as task


# Para a justificativa quanto à existência dessa task,
# vide comentários no arquivo de flows
@task
def create_DO_params_dict(environment: str = "dev", date: Optional[str] = None):
    return {"environment": environment, "date": date}


@task
def create_dbt_params_dict(environment: str = "dev"):
    # Queremos executar o seguinte comando:
    # $ dbt build --select +tag:cdi+ --target ENV
    return {
        "environment": environment,
        "rename_flow": False,
        "send_discord_report": False,
        "command": "build",
        "select": "+tag:cdi+",
        "exclude": None,
        "flag": None
    }
