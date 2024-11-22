# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
VitaCare Prontuario Backup dump flows
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun, VertexRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_db.constants import (
    constants as vitacare_db_constants,
)
from pipelines.datalake.extract_load.vitacare_db.tasks import (
    check_duplicated_files,
    check_filename_format,
    check_missing_or_extra_files,
    create_parquet_file,
    create_temp_database,
    delete_temp_database,
    generate_filters,
    get_backup_date,
    get_backup_file,
    get_bucket_name,
    get_connection_string,
    get_database_name,
    get_file_names,
    get_queries,
    unzip_file,
    upload_backups_to_cloud_storage,
    upload_many_to_datalake,
)
from pipelines.datalake.utils.data_extraction.google_drive import dowload_from_gdrive
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.prontuarios.utils.tasks import get_healthcenter_name_from_cnes
from pipelines.utils.tasks import create_folders

with Flow(name="DataLake - Extração e Carga de Dados - VitaCare DB") as sms_dump_vitacare_db:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # VITACARE DB
    CNES = Parameter("cnes", default=None, required=True)
    BACKUP_SUBFOLDER = Parameter("backup_subfolder", default=None, required=True)

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)
    UPLOAD_ONLY_EXPECTED_FILES = Parameter("upload_only_expected_files", default=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    with case(RENAME_FLOW, True):
        healthcenter_name = get_healthcenter_name_from_cnes(cnes=CNES)

        rename_current_flow_run(
            environment=ENVIRONMENT,
            unidade=healthcenter_name,
            cnes=CNES,
            backup_subfolder=BACKUP_SUBFOLDER,
        )

    ######################################
    # Tasks section #1 - Get Backup file
    ######################################

    bucket_name = get_bucket_name(env=ENVIRONMENT)

    backup_file = get_backup_file(
        bucket_name=bucket_name,
        backup_subfolder=BACKUP_SUBFOLDER,
        cnes=CNES,
    )

    backup_date = get_backup_date(file_name=backup_file)

    ######################################
    # Tasks section #2 - Create Temp Database
    ######################################

    connection_string = get_connection_string(environment=ENVIRONMENT)

    database_name = get_database_name(cnes=CNES)

    temp_db = create_temp_database(
        database_host=connection_string["host"],
        database_port=connection_string["port"],
        database_user=connection_string["user"],
        database_password=connection_string["password"],
        database_name=database_name,
        backup_file=backup_file,
        upstream_tasks=[
            backup_file,
            backup_date,
            database_name,
        ],
    )

    ######################################
    # Tasks section #3 - Create Parquet Files
    ######################################

    queries = get_queries(database_name=database_name, upstream_tasks=[temp_db])

    filenames = get_file_names(upstream_tasks=[temp_db])

    parquet_files = create_parquet_file.map(
        database_host=unmapped(connection_string["host"]),
        database_port=unmapped(connection_string["port"]),
        database_user=unmapped(connection_string["user"]),
        database_password=unmapped(connection_string["password"]),
        database_name=unmapped(database_name),
        sql=queries,
        base_path=unmapped(local_folders["raw"]),
        filename=filenames,
        backup_date=unmapped(backup_date),
        upstream_tasks=[unmapped(temp_db), unmapped(queries)],
    )

    ######################################
    # Tasks section #4 - Upload data to BQ
    ######################################

    upload_to_datalake_task = upload_many_to_datalake(
        input_path=parquet_files,
        dataset_id=DATASET_ID,
        source_format="parquet",
        if_exists="replace",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[parquet_files],
    )

    ######################################
    # Tasks section #4 - Delete Temp Database
    ######################################

    delete_db = delete_temp_database(
        database_host=connection_string["host"],
        database_port=connection_string["port"],
        database_user=connection_string["user"],
        database_password=connection_string["password"],
        database_name=database_name,
        upstream_tasks=[parquet_files, upload_to_datalake_task],
    )


sms_dump_vitacare_db.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_db.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare_db.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
    memory_request="4Gi",
)


with Flow(name="DataLake - Migração de Dados - VitaCare DB") as sms_migrate_vitacare_db:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")

    # GOOGLE DRIVE
    LAST_UPDATE_START_DATE = Parameter("last_update_start_date", default=None)
    LAST_UPDATE_END_DATE = Parameter("last_update_end_date", default=None)

    #####################################
    # Set environment
    ####################################

    # local_folders = create_folders()

    local_folders = {
        "raw": "./data/raw",
        "partition_directory": "./data/partition_directory",
    }

    bucket_name = get_bucket_name(env=ENVIRONMENT)

    filters = generate_filters(
        last_update_start_date=LAST_UPDATE_START_DATE, last_update_end_date=LAST_UPDATE_END_DATE
    )

    ####################################
    # Tasks section #1 - Extract data
    ####################################

    raw_files = dowload_from_gdrive(
        folder_id=vitacare_db_constants.GDRIVE_FOLDER_ID.value,
        destination_folder=local_folders["raw"],
        file_extension="zip",
        look_in_subfolders=True,
        filter_type="last_updated",
        filter_param=filters,
    )

    with case(raw_files["has_data"], True):
        unziped_files = unzip_file(
            folder_path=local_folders["raw"],
            upstream_tasks=[raw_files],
        )

        valid_files = check_filename_format(files=unziped_files, upstream_tasks=[unziped_files])

        deduplicated_files = check_duplicated_files(files=valid_files, upstream_tasks=[valid_files])

        files_to_upload = check_missing_or_extra_files(
            files=deduplicated_files,
            return_only_expected=UPLOAD_ONLY_EXPECTED_FILES,
            upstream_tasks=[deduplicated_files],
        )

    #####################################
    # Tasks section #2 - Load data
    #####################################

    uploaded_files = upload_backups_to_cloud_storage(
        files=files_to_upload,
        staging_folder=local_folders["partition_directory"],
        bucket_name=bucket_name,
        upstream_tasks=[files_to_upload],
    )

# Storage and run configs
sms_migrate_vitacare_db.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_migrate_vitacare_db.run_config = VertexRun(
    image=constants.DOCKER_VERTEX_IMAGE.value,
    labels=[
        constants.RJ_SMS_VERTEX_AGENT_LABEL.value,
    ],
    # https://cloud.google.com/vertex-ai/docs/training/configure-compute#machine-types
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": constants.INFISICAL_TOKEN.value,
    },
)
