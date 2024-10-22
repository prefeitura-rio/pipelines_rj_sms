# -*- coding: utf-8 -*-
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitai_db.constants import constants as vitai_constants
from pipelines.misc.historical_vitai_db.constants import constants as historical_vitai_constants
from pipelines.datalake.extract_load.vitai_db.tasks import (
    create_folder,
    get_bigquery_project_from_environment,
    get_current_flow_labels,
    load_data_from_vitai_table,
)
from pipelines.misc.historical_vitai_db.tasks import (
    build_param_list,
)
from pipelines.prontuarios.utils.tasks import get_project_name, rename_current_flow_run
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.tasks import upload_to_datalake
from pipelines.utils.tasks import create_partitions, get_secret_key
from pipelines.utils.progress import (
    load_operators_progress,
    save_operator_progress,
    get_remaining_operators,
)
from pipelines.utils.basics import (
    as_dict,
    as_datetime,
)


with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Batch",
) as sms_dump_vitai_rio_saude_batch:
    #####################################
    # Tasks section #0 - Params
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    OPERATOR_KEY = Parameter("operator_key", default="")
    TABLE_NAME = Parameter("table_name", default="")
    SCHEMA_NAME = Parameter("schema_name", default="basecentral")
    DT_COLUMN = Parameter("datetime_column", default="datahora")
    TARGET_NAME = Parameter("target_name", default="")
    INTERVAL_START = Parameter("interval_start", default=None)
    INTERVAL_END = Parameter("interval_end", default=None)

    #####################################
    # Tasks section #1 - Setup Environment
    #####################################
    rename_current_flow_run(
        name_template="""Operário '{operator_key}'""",  # noqa
        operator_key=OPERATOR_KEY,
    )

    #####################################
    # Tasks section #2 - Extraction Preparation
    #####################################
    db_url = get_secret_key(
        environment=ENVIRONMENT, secret_name="DB_URL", secret_path="/prontuario-vitai"
    )

    project_name = get_project_name(environment=ENVIRONMENT)
    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    raw_folder = create_folder(title="raw", subtitle=TARGET_NAME)
    partition_folder = create_folder(title="partition_directory", subtitle=TARGET_NAME)

    table_info = as_dict(
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        datetime_column=DT_COLUMN,
        target_name=TARGET_NAME,
    )

    #####################################
    # Tasks section #4 - Downloading Table Data
    #####################################
    file_list = load_data_from_vitai_table(
        db_url=db_url,
        table_info=table_info,
        output_file_folder=raw_folder,
        interval_start=as_datetime(date_str=INTERVAL_START),
        interval_end=as_datetime(date_str=INTERVAL_END),
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
        dataset_id=vitai_constants.DATASET_NAME.value,
        if_exists="replace",
        source_format="parquet",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[create_partitions_task],
    )

    #####################################
    # Tasks section #7 - Saving Progress
    #####################################
    save_operator_progress(
        operator_key=OPERATOR_KEY,
        slug=historical_vitai_constants.SLUG_NAME.value,
        project_name=bigquery_project,
        upstream_tasks=[upload_to_datalake_task],
    )


sms_dump_vitai_rio_saude_batch.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitai_rio_saude_batch.executor = LocalDaskExecutor(num_workers=6)
sms_dump_vitai_rio_saude_batch.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Histórico",
) as vitai_db_historical:
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    WINDOW_SIZE = Parameter("window_size", default=7)

    TABLE_NAME = Parameter("table_name", default="")
    SCHEMA_NAME = Parameter("schema_name", default="basecentral")
    DT_COLUMN = Parameter("datetime_column", default="datahora")
    TARGET_NAME = Parameter("target_name", default="")

    rename_current_flow_run(
        name_template="""Manager '{schema_name}.{table_name}'- Janela:{window_size} ({environment})""",  # noqa
        schema_name=SCHEMA_NAME,
        table_name=TABLE_NAME,
        window_size=WINDOW_SIZE,
        environment=ENVIRONMENT,
    )

    progress_table = load_operators_progress(
        slug=historical_vitai_constants.SLUG_NAME.value,
        project_name=ENVIRONMENT
    )

    params = build_param_list(
        environment=ENVIRONMENT,
        table_name=TABLE_NAME,
        schema_name=SCHEMA_NAME,
        datetime_column=DT_COLUMN,
        target_name=TARGET_NAME,
        window_size=WINDOW_SIZE,
    )

    remaining_runs = get_remaining_operators(
        progress_table=progress_table,
        all_operators_params=params
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    current_flow_run_labels = get_current_flow_labels()

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Batch"),
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

vitai_db_historical.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_db_historical.executor = LocalDaskExecutor(num_workers=1)
vitai_db_historical.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)
