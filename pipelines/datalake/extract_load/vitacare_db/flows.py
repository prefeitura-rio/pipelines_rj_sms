# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
VitaCare Prontuario Backup dump flows
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants

from pipelines.datalake.extract_load.vitacare_db.tasks import (
    create_parquet_file,
    create_temp_database,
    delete_temp_database,
    get_backup_date,
    get_backup_file,
    get_bucket_name,
    get_connection_string,
    get_database_name,
    get_file_names,
    get_queries,
    upload_many_to_datalake,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.sms import get_healthcenter_name_from_cnes
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
    UPLOAD_IF_TABLE_IS_MISSING = Parameter("upload_if_table_is_missing", default=False)

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)

    #####################################
    # Set environment
    ####################################
    connection_string = get_connection_string(environment=ENVIRONMENT)

    local_folders = create_folders(upstream_tasks=[connection_string])

    with case(RENAME_FLOW, True):
        healthcenter_name = get_healthcenter_name_from_cnes(
            cnes=CNES, upstream_tasks=[connection_string]
        )

        rename_current_flow_run(
            environment=ENVIRONMENT,
            unidade=healthcenter_name,
            cnes=CNES,
            backup_subfolder=BACKUP_SUBFOLDER,
            upstream_tasks=[healthcenter_name],
        )

    ######################################
    # Tasks section #1 - Get Backup file
    ######################################

    bucket_name = get_bucket_name(env=ENVIRONMENT, upstream_tasks=[connection_string])

    backup_file = get_backup_file(
        bucket_name=bucket_name,
        backup_subfolder=BACKUP_SUBFOLDER,
        cnes=CNES,
        upstream_tasks=[bucket_name],
    )

    backup_date = get_backup_date(file_name=backup_file, upstream_tasks=[backup_file])

    ######################################
    # Tasks section #2 - Create Temp Database
    ######################################

    database_name = get_database_name(cnes=CNES, upstream_tasks=[connection_string])

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
        extract_if_table_is_missing=unmapped(UPLOAD_IF_TABLE_IS_MISSING),
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
        upload_if_table_is_missing=UPLOAD_IF_TABLE_IS_MISSING,
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
        constants.RJ_SMS_AGENT_LABEL__DADOSRIO_CLUSTER.value,
    ],
    memory_limit="8Gi",
    memory_request="8Gi",
)
