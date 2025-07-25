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
    build_operator_params,
    consolidate_cnes_table_results,
    create_and_send_final_report,
    get_tables_to_extract,
    get_vitacare_cnes_from_bigquery,
    process_cnes_table,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_project_name,
    get_secret_key,
    rename_current_flow_run,
)

with Flow(
    "Datalake - Extração e Carga de Dados - Vitacare (Cloud SQL) - Operator V2",
    state_handlers=[handle_flow_state_change],
    owners=[
        global_constants.DANIEL_ID.value,
    ],
) as flow_vitacare_historic_table_operator_v2:

    TABLE_NAME = Parameter("TABLE_NAME", required=True)
    ENVIRONMENT = Parameter("environment", default="staging", required=True)
    DB_SCHEMA = Parameter("DB_SCHEMA", default=vitacare_constants.DB_SCHEMA.value)
    PARTITION_COLUMN = Parameter(
        "PARTITION_COLUMN", default=vitacare_constants.BQ_PARTITION_COLUMN.value
    )
    RENAME_FLOW = Parameter("RENAME_FLOW", default=True)

    DATASET_ID = vitacare_constants.DATASET_ID.value
    SECRET_PATH = vitacare_constants.INFISICAL_PATH.value

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            name_template="Tabela {table_name} ({env})",
            table_name=TABLE_NAME,
            env=ENVIRONMENT,
        )

    db_host = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=vitacare_constants.INFISICAL_HOST.value,
        environment=ENVIRONMENT,
    )
    db_port = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=vitacare_constants.INFISICAL_PORT.value,
        environment=ENVIRONMENT,
    )
    db_user = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=vitacare_constants.INFISICAL_USERNAME.value,
        environment=ENVIRONMENT,
    )
    db_password = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=vitacare_constants.INFISICAL_PASSWORD.value,
        environment=ENVIRONMENT,
    )

    cnes_to_process = get_vitacare_cnes_from_bigquery()

    cnes_processing_statuses = process_cnes_table.map(
        db_host=unmapped(db_host),
        db_port=unmapped(db_port),
        db_user=unmapped(db_user),
        db_password=unmapped(db_password),
        db_schema=unmapped(DB_SCHEMA),
        db_table=unmapped(TABLE_NAME),
        dataset_id=unmapped(DATASET_ID),
        partition_column=unmapped(PARTITION_COLUMN),
        cnes_code=cnes_to_process,
    )

    table_summary_result = consolidate_cnes_table_results(cnes_processing_statuses)

    flow_vitacare_historic_table_operator_v2.result = table_summary_result

with Flow(
    "Datalake - Extração e Carga de Dados - Vitacare (Cloud SQL) - Manager V2",
    state_handlers=[handle_flow_state_change],
    owners=[
        global_constants.DANIEL_ID.value,
    ],
) as flow_vitacare_historic_manager_v2:

    ENVIRONMENT = Parameter("environment", default="staging")
    DB_SCHEMA = Parameter("DB_SCHEMA", default=vitacare_constants.DB_SCHEMA.value)

    tables_to_process = get_tables_to_extract()

    project_name = get_project_name(environment=ENVIRONMENT)

    current_labels = get_current_flow_labels()

    operator_parameters = build_operator_params(
        tables=tables_to_process,
        env=ENVIRONMENT,
        schema=DB_SCHEMA,
        part_col=vitacare_constants.BQ_PARTITION_COLUMN.value,
    )

    created_operator_runs = create_flow_run.map(
        flow_name=unmapped(flow_vitacare_historic_table_operator_v2.name),
        project_name=unmapped(project_name),
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

    all_tables_summaries = wait_for_flow_run.map(
        flow_run_id=created_operator_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(False),
        return_result=unmapped(True),
    )

    create_and_send_final_report(all_tables_summaries)


flow_vitacare_historic_manager_v2.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic_manager_v2.executor = LocalDaskExecutor(num_workers=5)
flow_vitacare_historic_manager_v2.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="2Gi",
    memory_request="1Gi",
)

flow_vitacare_historic_table_operator_v2.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic_table_operator_v2.executor = LocalDaskExecutor(num_workers=1)
flow_vitacare_historic_table_operator_v2.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="8Gi",
    memory_request="8Gi",
)

flow_vitacare_historic_manager_v2.schedule = vitacare_backup_manager_schedule
