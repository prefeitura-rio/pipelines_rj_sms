# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitacare healthrecord dumping flows
"""
from prefect import Flow
from pipelines.tasks import (
    execute_dbt
)

with Flow(
    name="SMS: Test DBT - Executar uma query dbt"
) as test_execute_dbt:
    
    execute_dbt = execute_dbt(
        command='run',
        model='raw_cnes__estabelecimento'
    )
