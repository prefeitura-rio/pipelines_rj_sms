# -*- coding: utf-8 -*-
# pylint: disable=import-error

"""
Flow para migração de dados do BigQuery para o MySQL da SUBPAV.
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.migrate.bq_to_subpav.schedules import (
    bq_to_subpav_combined_schedule,
)
from pipelines.datalake.migrate.bq_to_subpav.tasks import (
    build_insert_config,
    generate_report,
    get_db_uri,
    insert_df_into_mysql,
    query_bq_table,
    resolve_notify,
    apply_df_filter
    
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="DataLake - Migration - BigQuery to MySQL SUBPAV",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DAYANE_ID.value],
) as bq_to_subpav_flow:

    #####################################
    # Parameters
    #####################################
    INFISICAL_PATH = Parameter("infisical_path", default="/smsrio")
    SECRET_NAME = Parameter("secret_name", default="DB_URL")
    DATASET_ID = Parameter("dataset_id", required=True)
    TABLE_ID = Parameter("table_id", required=True)
    DB_SCHEMA = Parameter("db_schema", required=True)
    BQ_COLUMNS = Parameter("bq_columns", required=False, default=None)
    PROJECT = Parameter("project", required=True)
    IF_EXISTS = Parameter("if_exists", default="append")
    DEST_TABLE = Parameter("dest_table", required=True)
    CUSTOM_INSERT_QUERY = Parameter("custom_insert_query", required=False, default=None)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    ENVIRONMENT = Parameter("environment", default="dev")
    LIMIT = Parameter("limit", required=False, default=None)
    BATCH_SIZE = Parameter("batch_size", required=False, default=1000)
    DF_FILTER_NAME = Parameter("df_filter_name", required=False, default=None)

    NOTIFY = Parameter("notify", default=None)

    NOTIFY_RESOLVED = resolve_notify(project=PROJECT, notify_param=NOTIFY)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            dataset=DATASET_ID,
            table=TABLE_ID,
        )

    #####################################
    # Tasks
    #####################################
    result = query_bq_table(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        environment=ENVIRONMENT,
        bq_columns=BQ_COLUMNS,
        limit=LIMIT,
    )

    df_bq = result["df"]
    metrics = result["metrics"]

    df_bq = apply_df_filter(
        df=df_bq, 
        filter_name=DF_FILTER_NAME, 
        notes=metrics["notes"]
        )

    db_uri = get_db_uri(
        infisical_path=INFISICAL_PATH,
        secret_name=SECRET_NAME,
        environment=ENVIRONMENT,
    )

    config = build_insert_config(
        db_schema=DB_SCHEMA,
        table_name=DEST_TABLE,
        if_exists=IF_EXISTS,
        custom_insert_query=CUSTOM_INSERT_QUERY,
        environment=ENVIRONMENT,
        batch_size=BATCH_SIZE,
    )

    result = insert_df_into_mysql(
        df=df_bq,
        mysql_uri=db_uri,
        config=config,
        metrics=metrics,
    )

    generate_report(
        metrics=result,
        context={
            "project": PROJECT,
            "notify": NOTIFY_RESOLVED,
            "environment": ENVIRONMENT,
        },
    )
#####################################
# Configuração do Flow
#####################################
bq_to_subpav_flow.executor = LocalDaskExecutor(num_workers=10)
bq_to_subpav_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
bq_to_subpav_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="4Gi",
    memory_request="4Gi",
)
bq_to_subpav_flow.schedule = bq_to_subpav_combined_schedule
