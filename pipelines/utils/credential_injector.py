# -*- coding: utf-8 -*-
from typing import Any, Callable, Union

import prefect
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.utils.infisical import inject_bd_credentials


def authenticated_task(fn: Callable = None, **task_init_kwargs: Any) -> Union[
    prefect.tasks.core.function.FunctionTask,
    Callable[[Callable], prefect.tasks.core.function.FunctionTask],
]:
    """
    A function that can be used to create a Prefect task.
    It injects the GCP credentials before executing the function.

    Mode 1: Standard Mode
    - If `fn` is not None, it creates a FunctionTask from `fn` and `task_init_kwargs`.

    Mode 2: Decorator Mode
    - If `fn` is None, it returns a decorator that can be used to create a Prefect task.
    - This case is used when we want to create a Prefect task from a function using @task().
    """

    def inject_credential_setting_in_function(function):
        """
        Receives a function and return a new version of it that injects the BD credentials
        in the beginning.
        """

        def new_function(**kwargs):
            assert "environment" in prefect.context.get(
                "parameters"
            ), "Environment not found in flow parameters"

            logger = prefect.context.get("logger")
            env = prefect.context.get("parameters")["environment"]
            logger.debug(f"[Injected] Set BD credentials for environment {env}")
            inject_bd_credentials(environment=env)

            logger.debug("[Injected] Now executing function normally...")
            return function(**kwargs)

        new_function.__name__ = function.__name__

        return new_function

    # Standard Mode: only create a FunctionTask from function
    if fn is not None:
        return prefect.tasks.core.function.FunctionTask(
            fn=inject_credential_setting_in_function(fn), **task_init_kwargs
        )
    # Decorator Mode: create a decorator that can be used to create a Prefect task
    else:
        return lambda any_function: prefect.tasks.core.function.FunctionTask(
            fn=inject_credential_setting_in_function(any_function),
            **task_init_kwargs,
        )


@authenticated_task()
def authenticated_create_flow_run(**kwargs):
    """
    Creates a flow run using the provided keyword arguments.

    Args:
        **kwargs: Keyword arguments to be passed to the `create_flow_run.run` function.

    Returns:
        Any: The result of the `create_flow_run.run` function.

    """
    logger = prefect.context.get("logger")
    logger.debug(f"Created Flow Run with params: {kwargs}")
    return create_flow_run.run(**kwargs)


@authenticated_task()
def authenticated_wait_for_flow_run(**kwargs):
    """
    Waits for a flow run using the provided keyword arguments.

    Args:
        **kwargs: Keyword arguments to be passed to the `wait_for_flow_run.run` function.

    Returns:
        Any: The result of the `wait_for_flow_run.run` function.

    """
    logger = prefect.context.get("logger")
    logger.debug(f"Waiting Flow Run with params: {kwargs}")
    return wait_for_flow_run.run(**kwargs)
