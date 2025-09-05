# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows de extração e carga de dados do Vitacare Historico SQL Server para o BigQuery
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants as global_constants
from pipelines.datalake.extract_load.vitacare_historico.constants import (
    vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_historico.schedules import (
    vitacare_historico_manager_schedule,
)
from pipelines.datalake.extract_load.vitacare_historico.tasks import (
    build_dbt_paramns,
    build_operator_params,
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
    owners=[global_constants.DANIEL_ID.value],
) as flow_vitacare_historic_operator_v2:

    TABLE_NAME = Parameter("table_name", required=True)
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    DB_SCHEMA = Parameter("db_schema", default=vitacare_constants.DB_SCHEMA.value)
    PARTITION_COLUMN = Parameter(
        "partition_column", default=vitacare_constants.BQ_PARTITION_COLUMN.value
    )
    RENAME_FLOW = Parameter("rename_flow", default=True)

    DATASET_ID = vitacare_constants.DATASET_ID.value
    SECRET_PATH = vitacare_constants.INFISICAL_PATH.value

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

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            name_template="Tabela {table_name} ({env})",
            table_name=TABLE_NAME,
            env=ENVIRONMENT,
        )

    cnes_to_process = ["5154197_20250902_175554"]

    process_cnes_table.map(
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

with Flow(
    "Datalake - Extração e Carga de Dados - Vitacare (Cloud SQL) - Manager V2",
    state_handlers=[handle_flow_state_change],
    owners=[global_constants.DANIEL_ID.value],
) as flow_vitacare_historic_manager_v2:

    ENVIRONMENT = Parameter("environment", default="staging")
    DB_SCHEMA = Parameter("db_schema", default=vitacare_constants.DB_SCHEMA.value)
    SKIP_DBT_RUN = Parameter("skip_dbt_run", default=True)

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
        flow_name=unmapped(flow_vitacare_historic_operator_v2.name),
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

    dbt_params = build_dbt_paramns(env=ENVIRONMENT)

    with case(SKIP_DBT_RUN, False):
        created_dbt_runs = create_flow_run(
            flow_name="DataLake - Transformação - DBT",
            project_name=project_name,
            parameters=dbt_params,
            labels=current_labels,
            upstream_tasks=[wait_for_operator_runs],
        )

    with case(SKIP_DBT_RUN, False):
        wait_for_dbt_runs = wait_for_flow_run.map(
            flow_run_id=created_dbt_runs,
            stream_states=True,
            stream_logs=True,
            raise_final_state=False,
        )


flow_vitacare_historic_manager_v2.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic_manager_v2.executor = LocalDaskExecutor(num_workers=6)
flow_vitacare_historic_manager_v2.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="3Gi",
    memory_request="3Gi",
)

flow_vitacare_historic_operator_v2.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic_operator_v2.executor = LocalDaskExecutor(num_workers=1)
flow_vitacare_historic_operator_v2.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="8Gi",
    memory_request="8Gi",
)

flow_vitacare_historic_manager_v2.schedule = vitacare_historico_manager_schedule
