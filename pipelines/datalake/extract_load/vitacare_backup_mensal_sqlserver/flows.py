# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows de extração e carga de dados do Vitacare Historic SQL Server para o BigQuery
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants as global_constants
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.schedules import (
    vitacare_backup_manager_schedule,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.tasks import (
    get_tables_to_extract,
    get_vitacare_cnes_from_bigquery,
    process_cnes_table,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.utils import (
    create_and_send_final_report,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.tasks import (
    get_project_name,
    get_secret_key,
    rename_current_flow_run,
)

with Flow("DataLake - Vitacare Historic - Table Operator") as flow_vitacare_historic_table_operator:
    TABLE_NAME = Parameter("TABLE_NAME", required=True)
    environment = Parameter("environment", default="staging", required=True)
    DB_SCHEMA = Parameter("DB_SCHEMA", default=vitacare_constants.DB_SCHEMA.value)
    PARTITION_COLUMN = Parameter(
        "PARTITION_COLUMN", default=vitacare_constants.BQ_PARTITION_COLUMN.value
    )
    RENAME_FLOW = Parameter("RENAME_FLOW", default=True)

    DATASET_ID = vitacare_constants.DATASET_ID.value

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            name_template="Operator: Vitacare Table {table_name} ({env})",
            table_name=TABLE_NAME,
            env=environment,
        )

    db_host = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_HOST.value,
        environment=environment,
    )
    db_port = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_PORT.value,
        environment=environment,
    )
    db_user = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_USERNAME.value,
        environment=environment,
    )
    db_password = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_PASSWORD.value,
        environment=environment,
    )

    all_cnes_to_process = get_vitacare_cnes_from_bigquery()

    etl_results = process_cnes_table.map(
        db_host=unmapped(db_host),
        db_port=unmapped(db_port),
        db_user=unmapped(db_user),
        db_password=unmapped(db_password),
        db_schema=unmapped(DB_SCHEMA),
        db_table=unmapped(TABLE_NAME),
        dataset_id=unmapped(DATASET_ID),
        partition_column=unmapped(PARTITION_COLUMN),
        cnes_code=all_cnes_to_process,
    )


with Flow("DataLake - Vitacare Historic - Manager") as flow_vitacare_historic_manager:
    environment = Parameter("environment", default="staging")
    DB_SCHEMA = Parameter("DB_SCHEMA", default=vitacare_constants.DB_SCHEMA.value)
    RENAME_FLOW = Parameter("RENAME_FLOW", default=True)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            name_template="Manager: Vitacare Backup ({env})",
            env=environment,
        )

    all_tables_to_process = get_tables_to_extract()

    prefect_project_name = get_project_name(environment=environment)
    current_labels = get_current_flow_labels()

    from prefect import task as prefect_task

    @prefect_task
    def build_operator_params(table_list: list, env: str, schema: str, part_col: str) -> list:
        params_list = []
        if not table_list:
            return []
        for table in table_list:
            params_list.append(
                {
                    "TABLE_NAME": table,
                    "environment": env,
                    "DB_SCHEMA": schema,
                    "PARTITION_COLUMN": part_col,
                    "RENAME_FLOW": True,
                }
            )
        return params_list

    operator_parameters = build_operator_params(
        table_list=all_tables_to_process,
        env=environment,
        schema=DB_SCHEMA,
        part_col=vitacare_constants.BQ_PARTITION_COLUMN.value,
    )

    created_operator_runs = create_flow_run.map(
        flow_name=unmapped(flow_vitacare_historic_table_operator.name),
        project_name=unmapped(prefect_project_name),
        parameters=operator_parameters,
        labels=unmapped(current_labels),
        run_name=unmapped(None),
    )

    wait_for_operator_runs = wait_for_flow_run.map(
        flow_run_id=created_operator_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(False),
    )

    create_and_send_final_report(operator_run_states=wait_for_operator_runs)


flow_vitacare_historic_manager.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic_manager.executor = LocalDaskExecutor(num_workers=1)
flow_vitacare_historic_manager.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="2Gi",
    memory_request="1Gi",
)

flow_vitacare_historic_table_operator.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic_table_operator.executor = LocalDaskExecutor(num_workers=2)
flow_vitacare_historic_table_operator.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="8Gi",
    memory_request="8Gi",
)

flow_vitacare_historic_manager.schedule = vitacare_backup_manager_schedule
