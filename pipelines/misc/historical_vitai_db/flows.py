# -*- coding: utf-8 -*-
from prefect import Parameter, case, flatten, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitai_db.constants import (
    constants as vitai_constants,
)
from pipelines.datalake.extract_load.vitai_db.schedules import (
    vitai_db_extraction_schedule,
)
from pipelines.datalake.extract_load.vitai_db.tasks import (
    create_datalake_table_name,
    create_folder,
    create_working_time_range,
    get_bigquery_project_from_environment,
    get_current_flow_labels,
    import_vitai_table,
)
from pipelines.misc.historical_vitai_db.tasks import (
    build_param_list,
    get_progress_table,
    save_progress,
    upload_folders_to_datalake,
    to_list,
)
from pipelines.prontuarios.utils.tasks import get_project_name, rename_current_flow_run
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.tasks import create_partitions, get_secret_key

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Batch",
) as sms_dump_vitai_rio_saude_batch:
    #####################################
    # Tasks section #1 - Setup Environment
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # In case the user want to force a interval
    INTERVAL_START = Parameter("interval_start", default=None)
    INTERVAL_END = Parameter("interval_end", default=None)
    TABLE_NAME = Parameter("table_name", default="paciente")

    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT)

    #####################################
    # Tasks section #2 - Extraction Preparation
    #####################################
    db_url = get_secret_key(
        environment=ENVIRONMENT, secret_name="DB_URL", secret_path="/prontuario-vitai"
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    tables_to_import = to_list(element=TABLE_NAME)

    datalake_table_names = create_datalake_table_name.map(table_name=tables_to_import)

    raw_folders = create_folder.map(title=unmapped("raw"), subtitle=datalake_table_names)
    partition_folders = create_folder.map(
        title=unmapped("partition_directory"), subtitle=datalake_table_names
    )

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    ####################################
    # Tasks section #3 - Interval Setup
    #####################################
    interval_start_per_table, interval_end_per_table = create_working_time_range(
        project_name=bigquery_project,
        dataset_name=vitai_constants.DATASET_NAME.value,
        table_names=datalake_table_names,
        interval_start=INTERVAL_START,
        interval_end=INTERVAL_END,
    )

    #####################################
    # Tasks section #4 - Downloading Table Data
    #####################################
    file_list_per_table = import_vitai_table.map(
        db_url=unmapped(db_url),
        table_name=tables_to_import,
        output_file_folder=raw_folders,
        interval_start=interval_start_per_table,
        interval_end=interval_end_per_table,
    )
    file_list = flatten(file_list_per_table)

    #####################################
    # Tasks section #5 - Partitioning Data
    #####################################
    create_partitions_task = create_partitions.map(
        data_path=raw_folders,
        partition_directory=partition_folders,
        upstream_tasks=[unmapped(file_list)],
        file_type=unmapped("parquet"),
    )

    #####################################
    # Tasks section #6 - Uploading to Datalake
    #####################################
    upload_to_datalake_task = upload_folders_to_datalake(
        input_path=partition_folders,
        table_id=datalake_table_names,
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
    save_progress_task = save_progress(
        slug="historico_vitai_db",
        environment=ENVIRONMENT,
        table=TABLE_NAME,
        interval_start=INTERVAL_START,
        interval_end=INTERVAL_END,
        upstream_tasks=[upload_to_datalake_task],
    )


sms_dump_vitai_rio_saude_batch.schedule = vitai_db_extraction_schedule
sms_dump_vitai_rio_saude_batch.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitai_rio_saude_batch.executor = LocalDaskExecutor(num_workers=5)
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
    TABLE_NAME = Parameter("table_name", default="paciente", required=True)
    WINDOW_SIZE = Parameter("window_size", default=7)

    progress_table = get_progress_table(slug="historico_vitai_db", environment=ENVIRONMENT)

    params = build_param_list(
        environment=ENVIRONMENT,
        table_name=TABLE_NAME,
        progress_table=progress_table,
        window_size=WINDOW_SIZE,
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    current_flow_run_labels = get_current_flow_labels()

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("Datalake - Extração e Carga de Dados - Vitai (Rio Saúde) - Batch"),
        project_name=unmapped(project_name),
        parameters=params,
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
