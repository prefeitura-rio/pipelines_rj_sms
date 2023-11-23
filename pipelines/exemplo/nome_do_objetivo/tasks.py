# -*- coding: utf-8 -*-
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log


@task
def greet(name: str = "world") -> None:
    log(f"Hello, {name}!")
