# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants as global_constants
from pipelines.datalake.extract_load.vitai_db.constants import (
    constants as vitai_db_constants,
)
from pipelines.datalake.extract_load.vitai_db.schedules import (
    vitai_db_extraction_schedule,
)
from pipelines.datalake.extract_load.vitai_db.tasks import (
    build_param_list,
    create_working_time_range,
    load_data_from_vitai_table,
)
from pipelines.utils.basics import as_dict, is_null_or_empty
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.progress import (
    get_remaining_operators,
    load_operators_progress,
    save_operator_progress,
)
from pipelines.utils.tasks import (
    create_folder,
    create_partitions,
    get_bigquery_project_from_environment,
    get_project_name,
    get_secret_key,
    rename_current_flow_run,
    upload_to_datalake,
)

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Operator",
) as datalake_extract_vitai_db_operator:
    #####################################
    # Tasks section #0 - Params
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    OPERATOR_KEY = Parameter("operator_key", default=None)
    TABLE_NAME = Parameter("table_name", default="")
    SCHEMA_NAME = Parameter("schema_name", default="basecentral")
    DT_COLUMN = Parameter("datetime_column", default="datahora")
    TARGET_NAME = Parameter("target_name", default="")
    INTERVAL_START = Parameter("interval_start", default=None)
    INTERVAL_END = Parameter("interval_end", default=None)
    PARTITION_COLUMN = Parameter("partition_column", default=None)

    #####################################
    # Tasks section #1 - Setup Environment
    #####################################
    with case(is_null_or_empty(value=OPERATOR_KEY), False):
        rename_current_flow_run(
            name_template="""Operário '{operator_key}'""",
            operator_key=OPERATOR_KEY,
        )
    with case(is_null_or_empty(value=OPERATOR_KEY), True):
        rename_current_flow_run(
            name_template="""Rotineiro '{schema_name}.{table_name} -> {target_name}'""",
            schema_name=SCHEMA_NAME,
            table_name=TABLE_NAME,
            target_name=TARGET_NAME,
        )

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    interval_start, interval_end = create_working_time_range(
        interval_start=INTERVAL_START,
        interval_end=INTERVAL_END,
    )

    #####################################
    # Tasks section #2 - Extraction Preparation
    #####################################
    db_url = get_secret_key(
        environment=ENVIRONMENT, secret_name="DB_URL", secret_path="/prontuario-vitai"
    )

    raw_folder = create_folder(title="raw", subtitle=TARGET_NAME)
    partition_folder = create_folder(title="partition_directory", subtitle=TARGET_NAME)

    table_info = as_dict(
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        datetime_column=DT_COLUMN,
        target_name=TARGET_NAME,
        partition_column=PARTITION_COLUMN,
    )

    #####################################
    # Tasks section #4 - Downloading Table Data
    #####################################
    file_list = load_data_from_vitai_table(
        db_url=db_url,
        table_info=table_info,
        output_file_folder=raw_folder,
        interval_start=interval_start,
        interval_end=interval_end,
    )

    #####################################
    # Tasks section #5 - Partitioning Data
    #####################################
    create_partitions_task = create_partitions(
        data_path=raw_folder,
        partition_directory=partition_folder,
        upstream_tasks=[file_list],
        file_type="parquet",
    )

    #####################################
    # Tasks section #6 - Uploading to Datalake
    #####################################
    upload_to_datalake_task = upload_to_datalake(
        input_path=partition_folder,
        table_id=TARGET_NAME,
        dataset_id=vitai_db_constants.DATASET_NAME.value,
        if_exists="replace",
        source_format="parquet",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        exception_on_missing_input_file=True,
        upstream_tasks=[create_partitions_task],
    )

    #####################################
    # Tasks section #7 - Saving Progress
    #####################################
    with case(is_null_or_empty(value=OPERATOR_KEY), False):
        save_operator_progress(
            operator_key=OPERATOR_KEY,
            slug=vitai_db_constants.SLUG_NAME.value,
            project_name=bigquery_project,
            upstream_tasks=[upload_to_datalake_task],
        )

datalake_extract_vitai_db_operator.schedule = vitai_db_extraction_schedule
datalake_extract_vitai_db_operator.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
datalake_extract_vitai_db_operator.executor = LocalDaskExecutor(num_workers=1)
datalake_extract_vitai_db_operator.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[
        global_constants.RJ_SMS_AGENT_LABEL_NEW.value,
    ],
    memory_limit="3Gi",
)

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Manager",
) as datalake_extract_vitai_db_manager:
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    WINDOW_SIZE = Parameter("window_size", default=7)

    TABLE_NAME = Parameter("table_name", default="")
    SCHEMA_NAME = Parameter("schema_name", default="basecentral")
    DT_COLUMN = Parameter("datetime_column", default="datahora")
    TARGET_NAME = Parameter("target_name", default="")
    PARTITION_COLUMN = Parameter("partition_column", default="datalake_loaded_at")

    rename_current_flow_run(
        name_template="""Manager '{schema_name}.{table_name}' - Janela: {window_size} ({environment})""",  # noqa
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        window_size=WINDOW_SIZE,
        environment=ENVIRONMENT,
    )

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    progress_table = load_operators_progress(
        slug=vitai_db_constants.SLUG_NAME.value, project_name=bigquery_project
    )

    params = build_param_list(
        environment=ENVIRONMENT,
        table_name=TABLE_NAME,
        schema_name=SCHEMA_NAME,
        datetime_column=DT_COLUMN,
        target_name=TARGET_NAME,
        window_size=WINDOW_SIZE,
        partition_column=PARTITION_COLUMN,
    )

    remaining_runs = get_remaining_operators(
        progress_table=progress_table, all_operators_params=params
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    current_flow_run_labels = get_current_flow_labels()

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Operator"),
        project_name=unmapped(project_name),
        parameters=remaining_runs,
        labels=unmapped(current_flow_run_labels),
    )

    wait_runs_task = wait_for_flow_run.map(
        flow_run_id=created_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

datalake_extract_vitai_db_manager.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
datalake_extract_vitai_db_manager.executor = LocalDaskExecutor(num_workers=1)
datalake_extract_vitai_db_manager.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[
        global_constants.RJ_SMS_AGENT_LABEL_NEW.value,
    ],
    memory_limit="2Gi",
)
