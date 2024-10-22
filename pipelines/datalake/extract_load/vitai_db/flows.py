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
    get_datalake_table_name,
    create_folder,
    create_working_time_range,
    get_bigquery_project_from_environment,
    get_current_flow_labels,
    load_data_from_vitai_table,
    list_tables_to_import,
    upload_folders_to_datalake,
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
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde)",
) as sms_dump_vitai_rio_saude:
    #####################################
    # Tasks section #1 - Setup Environment
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    INCREMENT_MODELS = Parameter("increment_models", default=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # In case the user want to force a interval
    INTERVAL_START = Parameter("interval_start", default=None)
    INTERVAL_END = Parameter("interval_end", default=None)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT)

    #####################################
    # Tasks section #2 - Extraction Preparation
    #####################################
    db_url = get_secret_key(
        environment=ENVIRONMENT, secret_name="DB_URL", secret_path="/prontuario-vitai"
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    tables_to_import = list_tables_to_import()

    datalake_table_names = get_datalake_table_name.map(table_info=tables_to_import)

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
        table_infos=tables_to_import,
        interval_start=INTERVAL_START,
        interval_end=INTERVAL_END,
    )

    #####################################
    # Tasks section #4 - Downloading Table Data
    #####################################
    file_list_per_table = load_data_from_vitai_table.map(
        db_url=unmapped(db_url),
        table_info=tables_to_import,
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
        input_paths=partition_folders,
        table_ids=datalake_table_names,
        dataset_id=vitai_constants.DATASET_NAME.value,
        if_exists="replace",
        source_format="parquet",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[create_partitions_task],
    )

    #####################################
    # Tasks section #7 - Running DBT Models
    #####################################
    with case(INCREMENT_MODELS, True):
        current_flow_run_labels = get_current_flow_labels()

        dbt_run_flow = create_flow_run(
            flow_name="DataLake - Transformação - DBT",
            project_name=project_name,
            parameters={
                "command": "run",
                "select": "tag:vitai",
                "exclude": "tag:vitai_estoque",
                "environment": ENVIRONMENT,
                "rename_flow": True,
                "send_discord_report": False,
            },
            labels=current_flow_run_labels,
            upstream_tasks=[upload_to_datalake_task],
        )
        wait_for_flow_run(flow_run_id=dbt_run_flow)

sms_dump_vitai_rio_saude.schedule = vitai_db_extraction_schedule
sms_dump_vitai_rio_saude.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitai_rio_saude.executor = LocalDaskExecutor(num_workers=6)
sms_dump_vitai_rio_saude.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)
