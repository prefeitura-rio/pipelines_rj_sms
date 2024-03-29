# -*- coding: utf-8 -*-
# flake8: noqa: E203
# pylint: disable= C0301
import prefect
from prefect.client import Client
from prefeitura_rio.pipelines_utils.logging import log


from pipelines.datalake.utils.data_transformations import convert_str_to_date
from pipelines.utils.credential_injector import authenticated_task as task


@task
def rename_current_flow_run(environment: str, is_routine: bool = True, **kwargs) -> None:
    """
    Renames the current flow run with a new name based on the provided parameters.

    Parameters:
    - environment (str): The environment for which the flow run is being renamed.
    - target_date (date): The target date for the flow run.
    - is_routine (bool, optional): Indicates whether the flow run is a routine or a reprocess. Defaults to False.
    - **kwargs: Additional keyword arguments that can be used to provide more information for the flow run name.

    Returns:
    None

    Example:
        rename_current_flow_run(environment='prod', target_date=date(2022, 1, 1), is_routine=True, param1='value1', param2='value2')
    """

    flow_run_id = prefect.context.get("flow_run_id")

    title = "Routine" if is_routine else "Reprocess"

    params = [f"{key}={value}" for key, value in kwargs.items() if key != "target_date"]
    params.append(f"env={environment}")
    params = sorted(params)

    if "target_date" in kwargs:
        target_date = kwargs.get("target_date")
        target_date = convert_str_to_date(target_date)
    else:
        target_date = prefect.context.get("scheduled_start_time").date()

    flow_run_name = f"{title} ({', '.join(params)}): {target_date}"

    client = Client()
    client.set_flow_run_name(flow_run_id, flow_run_name)

    log(f"Flow run {flow_run_id} renamed to {flow_run_name}")
