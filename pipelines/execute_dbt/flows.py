# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
DBT flows
"""
from prefect import Flow, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.tasks import (
    execute_dbt
)
from pipelines.execute_dbt.schedules import every_day_at_six_am

with Flow(
    name="SMS: Run DBT - Executar o comando RUN no projeto queries-rj-sms"
) as execute_dbt_flow:
    
    command = Parameter("command", default='run', required=False)
    target = Parameter("target", default='dev', required=False)
    model = Parameter("model", default=None, required=False)

    # Tasks
    execute_dbt_task = execute_dbt(
       command=command,
       target=target,
       model=model
    )

# Storage and run configs
execute_dbt_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
execute_dbt_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)

#execute_dbt_flow.schedule = every_day_at_six_am
