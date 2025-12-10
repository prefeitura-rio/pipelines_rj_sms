# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.prontuario_gcs.constants import (
    constants as prontuario_constants,
)
from pipelines.datalake.extract_load.prontuario_gcs.tasks import (
    build_operator_parameters,
    create_temp_folders,
    delete_temp_folders,
    extract_openbase_data,
    extract_postgres_data,
    get_file,
    list_files_from_bucket,
    unpack_files,
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

######################################################################################
#                                   OPERATOR
######################################################################################

with Flow(
    name="DataLake - Extração e Carga de Dados - ProntuaRIO Backups (OPERATOR)",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as prontuario_extraction_operator:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    BUCKET_NAME = Parameter("bucket_name", default="subhue_backups", required=True)
    CNES = Parameter("cnes", required=True)
    RENAME_FLOW = Parameter("rename_flow", required=False)
    DATASET = Parameter("dataset", default="brutos_prontuario_prontuaRIO", required=True)
    BLOB_PREFIX = Parameter("blob_prefix", required=True)
    LINES_PER_CHUNK = Parameter("lines_per_chunk", default=25_000)
    SKIP_OPENBASE = Parameter("skip_openbase", default=False, required=False)
    SKIP_POSTGRES = Parameter("skip_postgres", default=False, required=False)

    with case(RENAME_FLOW, value=True):
        rename_current_flow_run(cnes=CNES, blob_prefix=BLOB_PREFIX)

    # 1 - Cria diretórios temporários
    folders_created = create_temp_folders(
        folders=[
            prontuario_constants.UPLOAD_PATH.value,
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        ]
    )

    #####################
    # 2 Extração OPENBASE
    #####################

    with case(SKIP_OPENBASE, False):
        # 2.1 - Faz o download do arquivo OpenBase
        openbase_file = get_file(
            path=prontuario_constants.DOWNLOAD_DIR.value,
            bucket_name=BUCKET_NAME,
            environment=ENVIRONMENT,
            blob_prefix=BLOB_PREFIX,
            wait_for=folders_created,
            blob_type="BASE",
        )

        # 2.2 - Descompressão dos arquivos
        unpacked_openbase = unpack_files(
            tar_files=openbase_file,
            output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            files_to_extract=prontuario_constants.SELECTED_BASE_FILES.value,
            exclude_origin=True,
            wait_for=openbase_file,
        )

        # 2.3 - Extração e upload (por chunk) das tabelas selecionadas dos arquivos OpenBase
        openbase_finished = extract_openbase_data(
            data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            output_dir=prontuario_constants.UPLOAD_PATH.value,
            cnes=CNES,
            environment=ENVIRONMENT,
            dataset_id=DATASET,
            lines_per_chunk=LINES_PER_CHUNK,
            wait_for=unpacked_openbase,
        )

    #####################
    # 3 Extração POSTGRES
    #####################

    with case(SKIP_POSTGRES, False):
        # 3.1 Download do tar com os arquivos POSTGRES
        postgres_file = get_file(
            path=prontuario_constants.DOWNLOAD_DIR.value,
            bucket_name=BUCKET_NAME,
            environment=ENVIRONMENT,
            blob_prefix=BLOB_PREFIX,
            wait_for=openbase_finished,
            blob_type="VISUAL",
        )

        # 3.2 - Descompressão do arquivo hospub.sql
        unpacked_hospub = unpack_files(
            tar_files=postgres_file,
            output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            files_to_extract=["hospub.sql"],
            exclude_origin=False,
            wait_for=postgres_file,
        )

        # 3.3 - Extração das tabelas do arquivo hospub.sql
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

        # 3.4 - Descompressão do arquivo prescricao_medica3.sql
        unpacked_prescricao = unpack_files(
            tar_files=postgres_file,
            output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            files_to_extract=["prescricao_medica3.sql"],
            exclude_origin=True,
            wait_for=hospub_extraction_finished,
        )

        # 3.5 Extração das tabelas do arquivo prescricao.sql
        prescricao_extraction_finished = extract_postgres_data(
            data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            output_dir=prontuario_constants.UPLOAD_PATH.value,
            wait_for=unpacked_prescricao,
            lines_per_chunk=LINES_PER_CHUNK,
            dataset_id=DATASET,
            cnes=CNES,
            environment=ENVIRONMENT,
            sql_file="prescricao_medica3.sql",
            target_tables=prontuario_constants.SELECTED_PRESCRICAO_TABLES.value,
        )

    # 4 - Deletar arquivos e diretórios
    delete_temp_folders(
        folders=[
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            prontuario_constants.UPLOAD_PATH.value,
        ],
        wait_for= openbase_finished if SKIP_POSTGRES else prescricao_extraction_finished,
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
    DATASET = Parameter("dataset", default="brutos_prontuario_prontuaRIO", required=True)
    FOLDER = Parameter("folder", default="", required=True)
    CHUNK_SIZE = Parameter("chunk_size", default=25_000)
    
    with case(RENAME_FLOW, True):
        rename_current_flow_run(folder=FOLDER, env=ENVIRONMENT)

    # 1 - Listar os arquivos no bucket
    prefix_p_cnes = list_files_from_bucket(
        environment=ENVIRONMENT, bucket_name=BUCKET_NAME, folder=FOLDER
    )

    # 2 - Criar os operators para cara CNES
    ## 2.1 Criar os parametros para cada flow
    operator_params = build_operator_parameters(
        files_per_cnes=prefix_p_cnes,
        bucket_name=BUCKET_NAME,
        dataset_id=DATASET,
        environment=ENVIRONMENT,
        chunk_size=CHUNK_SIZE,
    )

    # 2.2 Criar as flows runs para cada CNES
    prefect_project_name = get_project_name(environment=ENVIRONMENT)
    current_labels = get_current_flow_labels()

    created_operator_runs = create_flow_run.map(
        flow_name=unmapped(prontuario_extraction_operator.name),
        project_name=unmapped(prefect_project_name),
        labels=unmapped(current_labels),
        parameters=operator_params,
        run_name=unmapped(None),
    )

    # 2.3 Acompanhar cada operator pelo wait_for_flow
    wait_for_operator_runs = wait_for_flow_run.map(
        flow_run_id=created_operator_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

# Operator
prontuario_extraction_operator.executor = LocalDaskExecutor(num_workers=1)
prontuario_extraction_operator.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
prontuario_extraction_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="1Gi",
    memory_limit="8Gi",
)

# Manager
prontuario_extraction_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
prontuario_extraction_manager.executor = LocalDaskExecutor(num_workers=1)
prontuario_extraction_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="2Gi",
    memory_request="2Gi",
)
