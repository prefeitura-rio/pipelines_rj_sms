# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from typing import Optional

from pipelines.utils.credential_injector import authenticated_task as task


@task
def create_params_dict(environment: str = "dev", date: Optional[str] = None):
    return {"environment": environment, "date": date}
