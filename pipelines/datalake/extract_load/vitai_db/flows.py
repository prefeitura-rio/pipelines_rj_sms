# -*- coding: utf-8 -*-
from datetime import datetime

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants as global_constants

# Refatoração do Manager
from pipelines.datalake.extract_load.siscan_web_laudos.tasks import generate_extraction_windows
from pipelines.datalake.extract_load.vitai_db.constants import (
    constants as vitai_db_constants,
)
from pipelines.datalake.extract_load.vitai_db.schedules import (
    vitai_db_extraction_schedule,
)
from pipelines.datalake.extract_load.vitai_db.tasks import (
    build_param_list,
    create_working_time_range,
    define_queries,
    run_query,
    upload_to_native_table
)
from pipelines.datalake.utils.tasks import (
    extrair_fim,
    extrair_inicio,
    rename_current_flow_run
)
from pipelines.utils.basics import as_dict, is_null_or_empty
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.progress import save_operator_progress
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    create_folder,
    get_bigquery_project_from_environment,
    get_project_name,
    get_secret_key,
    rename_current_flow_run,
    upload_df_to_datalake,
)
from pipelines.utils.time import from_relative_date

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Operator",
    state_handlers=[handle_flow_state_change],
    owners=[
        global_constants.HERIAN_ID.value,
    ],
) as datalake_extract_vitai_db_operator:
    #####################################
    # Tasks section #0 - Params
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    OPERATOR_KEY = Parameter("operator_key", default=None)
    TABLE_NAME = Parameter("table_name", default="")
    SCHEMA_NAME = Parameter("schema_name", default="basecentral")
    DT_COLUMN = Parameter("datetime_column", default="datahora")
    DATASET_NAME = Parameter("dataset_name", default='brutos_prontuario_vitai_bc_staging')
    TARGET_NAME = Parameter("target_name", default="")
    INTERVAL_START = Parameter("interval_start", default=None)
    INTERVAL_END = Parameter("interval_end", default=None)
    PARTITION_COLUMN = Parameter("partition_column", default=None)
    BATCH_SIZE = Parameter("batch_size", default=10000)

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
    queries = define_queries(
        db_url=db_url,
        table_info=table_info,
        interval_start=interval_start,
        interval_end=interval_end,
        batch_size=BATCH_SIZE,
    )

    dataframes = run_query.map(
        db_url=unmapped(db_url),
        query=queries,
        partition_column=unmapped(PARTITION_COLUMN),
    )

    #####################################
    # Tasks section #5 - Partitioning Data
    #####################################
    upload_to_datalake_task = upload_to_native_table.map(
        df=dataframes,
        table_id=unmapped(TARGET_NAME),
        dataset_id=unmapped(DATASET_NAME),
        if_exists=unmapped("replace"),
        partition_column=unmapped(PARTITION_COLUMN)
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
datalake_extract_vitai_db_operator.executor = LocalDaskExecutor(num_workers=2)
datalake_extract_vitai_db_operator.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[
        global_constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
    memory_request="1Gi",
)

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Manager",
    state_handlers=[handle_flow_state_change],
    owners=[
        global_constants.HERIAN_ID.value,
    ],
) as datalake_extract_vitai_db_manager:

    ###########################
    # Parâmetros
    ###########################

    # Originais
    TABLE_NAME = Parameter("table_name", default="")
    SCHEMA_NAME = Parameter("schema_name", default="basecentral")
    DT_COLUMN = Parameter("datetime_column", default="datahora")
    DATASET_NAME = Parameter("dataset_name", default='brutos_prontuario_vitai_bc_staging')
    TARGET_NAME = Parameter("target_name", default="")
    PARTITION_COLUMN = Parameter("partition_column", default="datalake_loaded_at")

    # Refatoração
    ENVIRONMENT = Parameter("environment", default="dev")
    RELATIVE_DATE = Parameter("relative_date", default="M-1")
    DIAS_POR_OPERATOR = Parameter("range", default=7)
    RENAME_FLOW = Parameter("rename_flow", default=True)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            relative_date=RELATIVE_DATE,
            range=DIAS_POR_OPERATOR,
        )

    ###########################
    # Flow
    ###########################

    # Gera data relativa e a data atual
    relative_date = from_relative_date(relative_date=RELATIVE_DATE)
    today = datetime.now()

    # Gera as janelas de extração com base no interval
    windows = generate_extraction_windows(
        start_date=relative_date, end_date=today, interval=DIAS_POR_OPERATOR
    )
    interval_starts = extrair_inicio.map(faixa=windows)
    interval_ends = extrair_fim.map(faixa=windows)

    # Monta os parâmetros de cada operator
    prefect_project_name = get_project_name(environment=ENVIRONMENT)
    current_labels = get_current_flow_labels()

    # Adicionar os parâmetros do flow operator
    operator_params = build_param_list(
        datetime_column=DT_COLUMN,
        environment=ENVIRONMENT,
        end_dates=interval_ends,
        start_dates=interval_starts,
        partition_column=PARTITION_COLUMN,
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        target_name=TARGET_NAME,
        dataset_name=DATASET_NAME,
        rename_flow=RENAME_FLOW,
    )

    # Cria e espera a execução das flow runs
    created_operator_runs = create_flow_run.map(
        flow_name=unmapped(datalake_extract_vitai_db_operator.name),
        project_name=unmapped(prefect_project_name),
        labels=unmapped(current_labels),
        parameters=operator_params,
        run_name=unmapped(None),
    )

    wait_for_operator_runs = wait_for_flow_run.map(
        flow_run_id=created_operator_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

datalake_extract_vitai_db_manager.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
datalake_extract_vitai_db_manager.executor = LocalDaskExecutor(num_workers=1)
datalake_extract_vitai_db_manager.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[
        global_constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
