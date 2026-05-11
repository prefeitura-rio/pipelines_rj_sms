# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.prontuario_gcs.constants import (
    constants as prontuario_constants,
)
from pipelines.datalake.extract_load.prontuario_gcs.tasks import (
    build_openbase_parameters,
    build_postgres_parameters,
    create_temp_folders,
    delete_temp_folders,
    extract_openbase_data,
    extract_postgres_data,
    get_file,
    list_files_from_bucket,
    unpack_files,
    generate_current_folder
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
from pipelines.utils.tasks import get_project_name, rename_current_flow_run
from pipelines.datalake.extract_load.prontuario_gcs.schedules import schedule

######################################################################################
#                                 OPERATORS
######################################################################################

with Flow(
    name="DataLake - Extração e Carga de Dados - ProntuaRIO Backups OpenBase (OPERATOR)",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as prontuario_openbase_operator:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    BUCKET_NAME = Parameter("bucket_name", default="subhue_backups", required=True)
    CNES = Parameter("cnes", required=True)
    FOLDER = Parameter('folder', required=True)
    DATASET = Parameter("dataset", default="brutos_prontuario_prontuaRio_staging", required=True)
    LINES_PER_CHUNK = Parameter("lines_per_chunk", default=5_000)
    BLOB_PATH = Parameter("blob_path", required=True)

    rename_current_flow_run(folder=FOLDER, cnes=CNES)

    # 1 - Cria diretórios temporários
    folders_created = create_temp_folders(
        folders=[
            prontuario_constants.UPLOAD_PATH.value,
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        ]
    )

    # 2 - Faz o download do arquivo OpenBase
    openbase_file = get_file(
        path=prontuario_constants.DOWNLOAD_DIR.value,
        bucket_name=BUCKET_NAME,
        environment=ENVIRONMENT,
        blob_path=BLOB_PATH,
        wait_for=folders_created,
        blob_type="openbase",
    )

    # 3 - Descompressão dos arquivos
    unpacked_openbase = unpack_files(
        tar_files=openbase_file,
        output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        files_to_extract=prontuario_constants.SELECTED_BASE_FILES.value,
        exclude_origin=True,
        wait_for=openbase_file,
    )

    # 4 - Extração e upload (por chunk) das tabelas selecionadas dos arquivos OpenBase
    openbase_finished = extract_openbase_data(
        data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        output_dir=prontuario_constants.UPLOAD_PATH.value,
        cnes=CNES,
        environment=ENVIRONMENT,
        dataset_id=DATASET,
        lines_per_chunk=LINES_PER_CHUNK,
        tables_to_extract=prontuario_constants.SELECTED_OPENBASE_TABLES.value,
        wait_for=unpacked_openbase,
    )

    # 5 - Deletar arquivos e diretórios
    delete_temp_folders(
        folders=[
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            prontuario_constants.UPLOAD_PATH.value,
        ],
        wait_for=openbase_finished,
    )


with Flow(
    name="DataLake - Extração e Carga de Dados - ProntuaRIO Backups Postgres (OPERATOR)",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as prontuario_postgres_operator:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    BUCKET_NAME = Parameter("bucket_name", default="subhue_backups", required=True)
    CNES = Parameter("cnes", required=True)
    FOLDER = Parameter('folder', required=True)
    DATASET = Parameter("dataset", default="brutos_prontuario_prontuaRio_staging", required=True)
    LINES_PER_CHUNK = Parameter("lines_per_chunk", default=1_000)
    BLOB_PATH = Parameter("blob_path", required=True)

    rename_current_flow_run(folder=FOLDER, cnes=CNES)

    # 1 - Cria diretórios temporários
    folders_created = create_temp_folders(
        folders=[
            prontuario_constants.UPLOAD_PATH.value,
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        ]
    )

    # 2 - Download do tar com os arquivos POSTGRES
    postgres_file = get_file(
        path=prontuario_constants.DOWNLOAD_DIR.value,
        bucket_name=BUCKET_NAME,
        environment=ENVIRONMENT,
        blob_path=BLOB_PATH,
        wait_for=folders_created,
        blob_type="sql",
    )

    # 3 - Descompressão do arquivo hospub.sql
    unpacked_hospub = unpack_files(
        tar_files=postgres_file,
        output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        files_to_extract=["hospub.sql"],
        exclude_origin=True,
        wait_for=postgres_file,
    )

    # 4 - Extração das tabelas do arquivo hospub.sql
    hospub_extraction_finished = extract_postgres_data(
        data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        output_dir=prontuario_constants.UPLOAD_PATH.value,
        wait_for=unpacked_hospub,
        lines_per_chunk=LINES_PER_CHUNK,
        dataset_id=DATASET,
        cnes=CNES,
        environment=ENVIRONMENT,
        sql_file="hospub.sql",
        target_tables=prontuario_constants.SELECTED_HOSPUB_TABLES.value,
    )

    # 5 - Deletar arquivos e diretórios
    delete_temp_folders(
        folders=[
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            prontuario_constants.UPLOAD_PATH.value,
        ],
        wait_for=hospub_extraction_finished,
    )


######################################################################################
#                                     MANAGER
######################################################################################

with Flow(
    name="DataLake - Extração e Carga de Dados - ProntuaRIO Backups (MANAGER)",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as prontuario_extraction_manager:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    BUCKET_NAME = Parameter("bucket_name", default="subhue_backups", required=True)
    RENAME_FLOW = Parameter("rename_flow", required=False)
    DATASET = Parameter("dataset", default="brutos_prontuario_prontuaRio_staging", required=True)
    FOLDER = Parameter("folder", default="", required=False)
    CHUNK_SIZE = Parameter("chunk_size", default=1_000)

    folder = generate_current_folder(folder=FOLDER)
    rename_current_flow_run(folder=folder, env=ENVIRONMENT)
        
    # 1 - Listar os arquivos no bucket
    last_files = list_files_from_bucket(
        environment=ENVIRONMENT, bucket_name=BUCKET_NAME, folder=folder
    )

    # 2 - Criar os operators para cara CNES
    ## 2.1 Criar os parametros para cada flow
    openbase_params = build_openbase_parameters(
        last_files=last_files,
        folder=folder,
        bucket_name=BUCKET_NAME,
        dataset_id=DATASET,
        environment=ENVIRONMENT,
        chunk_size=CHUNK_SIZE,
    )

    postgres_params = build_postgres_parameters(
        last_files=last_files,
        folder=folder,
        bucket_name=BUCKET_NAME,
        dataset_id=DATASET,
        environment=ENVIRONMENT,
        chunk_size=CHUNK_SIZE,
    )

    prefect_project_name = get_project_name(environment=ENVIRONMENT)
    current_labels = get_current_flow_labels()

    # 2.2 Criar as flows runs para Openbase
    created_openbase_runs = create_flow_run.map(
        flow_name=unmapped(prontuario_openbase_operator.name),
        project_name=unmapped(prefect_project_name),
        labels=unmapped(current_labels),
        parameters=openbase_params,
        run_name=unmapped(None),
    )

    # 2.3 Criar as flows runs para Postgres
    created_postgres_runs = create_flow_run.map(
        flow_name=unmapped(prontuario_postgres_operator.name),
        project_name=unmapped(prefect_project_name),
        labels=unmapped(current_labels),
        parameters=postgres_params,
        run_name=unmapped(None),
    )

    # 2.4 Acompanhar cada operator Openbase pelo wait_for_flow
    wait_for_openbase_runs = wait_for_flow_run.map(
        flow_run_id=created_openbase_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

    # 2.5 Acompanhar cada operator Postgres pelo wait_for_flow
    wait_for_postgres_runs = wait_for_flow_run.map(
        flow_run_id=created_postgres_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )


# Operator OpenBase
prontuario_openbase_operator.executor = LocalDaskExecutor(num_workers=1)
prontuario_openbase_operator.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
prontuario_openbase_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="13Gi",
    memory_limit="13Gi",
)

# Operator Postgres
prontuario_postgres_operator.executor = LocalDaskExecutor(num_workers=1)
prontuario_postgres_operator.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
prontuario_postgres_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="13Gi",
    memory_limit="13Gi",
)

# Manager
prontuario_extraction_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
prontuario_extraction_manager.executor = LocalDaskExecutor(num_workers=1)
prontuario_extraction_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="4Gi",
    memory_request="2Gi",
)
prontuario_extraction_manager.schedule = schedule
