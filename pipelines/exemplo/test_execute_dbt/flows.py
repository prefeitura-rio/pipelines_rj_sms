# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitacare healthrecord dumping flows
"""
from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.tasks import (
    execute_dbt
)

with Flow(
    name="SMS: Test DBT - Executar uma query dbt"
) as test_execute_dbt_flow:
    

    # Tasks
    execute_dbt = execute_dbt(
        command='run',
        model='raw_cnes__estabelecimento'
    )

# Storage and run configs
test_execute_dbt_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
test_execute_dbt_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)