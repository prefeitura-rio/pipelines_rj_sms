# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
DBT flows
"""
from prefect import Flow
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
    

    # Tasks
    execute_dbt_task = execute_dbt(
        command='run',
        target='prod'
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
