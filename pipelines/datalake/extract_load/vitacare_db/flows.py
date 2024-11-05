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
    get_backup_filename,
    get_bucket_name,
    get_connection_string,
    get_database_name,
    get_file_names,
    get_queries,
    upload_many_to_datalake,
)
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

    # GCP
    DATASET_ID = Parameter("dataset_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    bucket_name = get_bucket_name(env=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        healthcenter_name = get_healthcenter_name_from_cnes(cnes=CNES)

        rename_current_flow_run(
            environment=ENVIRONMENT,
            unidade=healthcenter_name,
            cnes=CNES,
        )

    connection_string = get_connection_string(environment=ENVIRONMENT)

    backup_filename = get_backup_filename(bucket_name=bucket_name, cnes=CNES)

    database_name = get_database_name(cnes=CNES)

    filenames = get_file_names()

    ######################################
    # Tasks section #1 - Create Temp Database
    ######################################

    temp_db = create_temp_database(
        database_host=connection_string["host"],
        database_port=connection_string["port"],
        database_user=connection_string["user"],
        database_password=connection_string["password"],
        database_name=database_name,
        backup_filename=backup_filename,
    )

    ######################################
    # Tasks section #2 - Create Parquet Files
    ######################################

    queries = get_queries(database_name=database_name, upstream_tasks=[temp_db])

    parquet_files = create_parquet_file.map(
        database_host=unmapped(connection_string["host"]),
        database_port=unmapped(connection_string["port"]),
        database_user=unmapped(connection_string["user"]),
        database_password=unmapped(connection_string["password"]),
        database_name=unmapped(database_name),
        sql=queries,
        base_path=unmapped(local_folders["raw"]),
        filename=filenames,
        upstream_tasks=[unmapped(temp_db), unmapped(queries)],
    )

    ######################################
    # Tasks section #3 - Upload data to BQ
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
        upstream_tasks=[parquet_files],
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
