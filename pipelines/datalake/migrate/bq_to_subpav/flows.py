# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
# flake8: noqa E501

"""
Flow para migração de dados do BigQuery para o MySQL da SUBPAV.
"""

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.migrate.bq_to_subpav.schedules import (
    bq_to_subpav_combined_schedule,
)
from pipelines.datalake.migrate.bq_to_subpav.tasks import (
    insert_df_into_mysql,
    query_bq_table,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import get_secret_key

with Flow(name="DataLake - Migração de Dados - BigQuery para MySQL SUBPAV") as bq_to_subpav_flow:
    #####################################
    # Parameters
    #####################################

    # INFISICAL
    INFISICAL_PATH = Parameter("infisical_path", default="/smsrio")
    SECRET_NAME = Parameter("secret_name", default="DB_URL")
    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)
    # BQ
    DATASET_ID = Parameter("dataset_id", required=True)
    TABLE_ID = Parameter("table_id", required=True)
    PROJECT_ID = Parameter("project_id", required=True)
    SCHEMA = Parameter("db_schema", required=True)

    # DESTINO MySQL
    IF_EXISTS = Parameter("if_exists", default="append")
    DEST_TABLE = Parameter("dest_table", required=True)  # Nome da tabela destino no MySQL
    CUSTOM_INSERT_QUERY = Parameter("custom_insert_query", default=None)

    ENVIRONMENT = Parameter("environment", default="dev")

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            dataset=DATASET_ID,
            table=TABLE_ID,
        )

    #####################################
    # Tasks
    #####################################
    df_bq = query_bq_table(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        project_id=PROJECT_ID,
        environment=ENVIRONMENT,
    )

    insert_df_into_mysql(
        df=df_bq,
        infisical_path=INFISICAL_PATH,
        secret_name=SECRET_NAME,
        db_schema=SCHEMA,
        table_name=DEST_TABLE,
        if_exists=IF_EXISTS,
        environment=ENVIRONMENT,
        custom_insert_query=CUSTOM_INSERT_QUERY,
    )

#####################################
# Configuração do Flow
#####################################
bq_to_subpav_flow.executor = LocalDaskExecutor(num_workers=10)
bq_to_subpav_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="4Gi",
    memory_request="4Gi",
)
bq_to_subpav_flow.schedule = bq_to_subpav_combined_schedule
