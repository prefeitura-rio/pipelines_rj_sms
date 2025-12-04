# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
import os

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
    dumb_task,
    extract_openbase_data,
    extract_postgres_data,
    get_cnes_from_file_name,
    get_files,
    list_files_from_bucket,
    unpack_files,
    upload_to_datalake,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.flow import Flow
from pipelines.utils.logger import log
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_project_name,
    get_secret_key,
    rename_current_flow_run,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - ProntuaRIO Backups",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as prontuario_extraction_operator:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    BUCKET_NAME = Parameter("bucket_name", default="subhue_backups", required=True)
    CNES = Parameter("cnes", required=True)
    RENAME_FLOW = Parameter("rename_flow", required=False)
    DATASET = Parameter("dataset", default="brutos_prontuario_prontuaRIO", required=True)
    BLOB_PREFIX_LIST = Parameter("blob_prefix_list", required=True)
    LINES_PER_CHUNK = Parameter("lines_per_chunk", default=100_000)

    with case(RENAME_FLOW, value=True):
        rename_current_flow_run(name_template=f"Extração de arquivos de {CNES}")

    # 1 - Cria diretórios temporários
    folders_created = create_temp_folders(
        folders=[
            prontuario_constants.UPLOAD_PATH.value,
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        ]
    )

    # 2 - Faz o download dos arquivos no bucket
    downloaded_files = get_files(
        path=prontuario_constants.DOWNLOAD_DIR.value,
        bucket_name=BUCKET_NAME,
        environment=ENVIRONMENT,
        blob_prefixes=BLOB_PREFIX_LIST,
        wait_for=folders_created,
    )

    # 3 - Faz a descompressão dos arquivos
    output = unpack_files(
        tar_files=downloaded_files,
        output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        environment=ENVIRONMENT,
    )

    # 4 - Extração de tabelas selecionadas dos arquivos dump database
    postgres_finished = extract_postgres_data(
        data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        output_dir=prontuario_constants.UPLOAD_PATH.value,
        wait_for=output,
    )

    # 5 - Extração das tabelas selecionadas dos arquivos OpenBase
    openbase_finished = extract_openbase_data(
        data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value,
        output_dir=prontuario_constants.UPLOAD_PATH.value,
        wait_for=output,
    )

    # 6 - Upload das tabelas para o datalake
    upload_finished = upload_to_datalake(
        upload_path=prontuario_constants.UPLOAD_PATH.value,
        dataset_id=DATASET,
        wait_for=[openbase_finished, postgres_finished],
        environment=ENVIRONMENT,
        cnes=CNES,
        lines_per_chunk=LINES_PER_CHUNK,
    )

    # 7 - Deletar arquivos e diretórios
    delete_temp_folders(
        folders=[
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            prontuario_constants.UPLOAD_PATH.value,
        ],
        wait_for=upload_finished,
    )


################
# FLOW MANAGER
################
with Flow(
    name="DataLake - Extração e Carga de Dados - ProntuaRIO Backups",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as prontuario_extraction_manager:

    # 1 - Listar os arquivos no bucket
    files = list_files_from_bucket(
        environment=ENVIRONMENT, bucket_name=BUCKET_NAME, folder="2025-11"
    )

    # 2 - Separar path por CNES
    prefix_p_cnes = get_cnes_from_file_name(files=files)

    # 3 - Criar os operators para cara CNES

    ## 3.1 Criar os parametros para cada flow
    operator_params = build_operator_parameters(
        files_per_cnes=prefix_p_cnes,
        bucket_name=BUCKET_NAME,
        dataset_id=DATASET,
        environment=ENVIRONMENT,
    )

    ## 3.2 Criar as flows runs para cada CNES
    prefect_project_name = get_project_name(environment=ENVIRONMENT)
    current_labels = get_current_flow_labels()

    operator_runs = create_flow_run.map(
        flow_name=unmapped(prontuario_extraction_operator.name),
        project_name=unmapped(prefect_project_name),
        labels=unmapped(current_labels),
        parameters=operator_params,
        run_name=unmapped(None),
    )

    ## 3.3 Acompanhar cada operator pelo wait_for_flow
    wait_for_operator_runs = wait_for_flow_run(
        flow_run_id=operator_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )
