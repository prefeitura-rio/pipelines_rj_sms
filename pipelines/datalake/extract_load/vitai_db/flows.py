# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped, flatten
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitai_db.constants import constants as vitai_constants
from pipelines.datalake.extract_load.vitai_db.schedules import (
    vitai_db_extraction_schedule,
)
from pipelines.datalake.extract_load.vitai_db.tasks import (
    create_datalake_table_name,
    get_bigquery_project_from_environment,
    get_interval_start_list,
    get_last_timestamp_from_tables,
    import_vitai_table_to_csv,
    list_tables_to_import,
)
from pipelines.prontuarios.utils.tasks import get_project_name, rename_current_flow_run
from pipelines.utils.tasks import (
    get_secret_key,
    is_equal,
    upload_to_datalake,
    create_folders,
    create_partitions,
)

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde)",
) as sms_dump_vitai_rio_saude:
    #####################################
    # Tasks section #1 - Setup Environment
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    INTERVAL_START = Parameter("interval_start", default=None)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT)

    folders = create_folders()

    #####################################
    # Tasks section #2 - Extraction Preparation
    #####################################
    db_url = get_secret_key(
        environment=ENVIRONMENT, secret_name="DB_URL", secret_path="/prontuario-vitai"
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    tables_to_import = list_tables_to_import()

    datalake_table_names = create_datalake_table_name.map(table_name=tables_to_import)

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    ####################################
    # Tasks section #3 - Interval Start Setup
    #####################################
    is_interval_start_none = is_equal(value=INTERVAL_START, target=None)

    with case(is_interval_start_none, True):
        most_recent_timestamp_per_table = get_last_timestamp_from_tables(
            project_name=bigquery_project,
            dataset_name=vitai_constants.DATASET_NAME.value,
            table_names=datalake_table_names,
            column_name="datahora",
        )
    with case(is_interval_start_none, False):
        received_intervals_start = get_interval_start_list(
            interval_start=INTERVAL_START, table_names=tables_to_import
        )

    intervals_start_per_table = merge(most_recent_timestamp_per_table, received_intervals_start)

    #####################################
    # Tasks section #3 - Downloading Table Data
    #####################################
    file_list_per_table = import_vitai_table_to_csv.map(
        db_url=unmapped(db_url),
        table_name=tables_to_import,
        output_file_folder=unmapped(folders["raw"]),
        interval_start=intervals_start_per_table,
    )
    file_list = flatten(file_list_per_table)
    
    #####################################
    # Tasks section #4 - Partitioning Data
    #####################################
    create_partitions_task = create_partitions(
        data_path=folders["raw"],
        partition_directory=folders["partition_directory"],
        upstream_tasks=[file_list],
    )

    #####################################
    # Tasks section #5 - Uploading to Datalake
    #####################################
    upload_to_datalake_task = upload_to_datalake.map(
        input_path=unmapped(folders["partition_directory"]),
        table_id=datalake_table_names,
        dataset_id=unmapped(vitai_constants.DATASET_NAME.value),
        if_exists=unmapped("replace"),
        source_format=unmapped("csv"),
        if_storage_data_exists=unmapped("replace"),
        biglake_table=unmapped(True),
        dataset_is_public=unmapped(False),
        upstream_tasks=[unmapped(create_partitions_task)],
    )

sms_dump_vitai_rio_saude.schedule = vitai_db_extraction_schedule
sms_dump_vitai_rio_saude.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitai_rio_saude.executor = LocalDaskExecutor(num_workers=1)
sms_dump_vitai_rio_saude.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)
