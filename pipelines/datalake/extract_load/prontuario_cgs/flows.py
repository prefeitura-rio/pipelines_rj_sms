# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
import os
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.utils.logger import log 
from pipelines.constants import constants
from pipelines.datalake.extract_load.prontuario_cgs.constants import constants as prontuario_constants
from pipelines.datalake.extract_load.prontuario_cgs.tasks import (
    create_temp_folders,
    get_file,
    unpack_files,
    extract_openbase_data,
    extract_postgres_data,
    upload,
    dumb_task,
    delete_temp_folders
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name='DataLake - Extração e Carga de Dados - ProntuaRIO Backups',
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value]
) as prontuario_extraction:
    
    ENVIRONMENT = Parameter('environment', default='dev', required=True)
    BUCKET_NAME = Parameter('bucket_name', default='subhue_backups', required=True)
    DATASET = 'brutos_prontuario_prontuaRIO'
    BLOB_PREFIX = '2025-11/hospub-2269945'
    
    # 1 - Cria diretórios temporários
    folders_created = create_temp_folders(
        folders=[
            prontuario_constants.UPLOAD_PATH.value,
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            ])

    #2 - Faz o download dos arquivos no bucket
    downloaded_files = get_file(
        path=prontuario_constants.DOWNLOAD_DIR.value,
        bucket_name=BUCKET_NAME,
        environment=ENVIRONMENT,
        blob_prefix=BLOB_PREFIX,
        wait_for=folders_created
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
        wait_for=output
    )
   
     # 5 - Extração das tabelas selecionadas dos arquivos OpenBase
    openbase_finished = extract_openbase_data(
        data_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value, 
        output_dir=prontuario_constants.UPLOAD_PATH.value,
        wait_for=output
    )

    # 6 - Upload das tabelas para o datalake
    upload_finished = upload(
        upload_path=prontuario_constants.UPLOAD_PATH.value,
        dataset_id=DATASET,
        wait_for = None,
        environment=ENVIRONMENT,
        cnes='2269945'
    )
    
    # 7 - Deletar arquivos e diretórios
    delete_temp_folders(
        folders=[
            prontuario_constants.DOWNLOAD_DIR.value,
            prontuario_constants.UNCOMPRESS_FILES_DIR.value,
            prontuario_constants.UPLOAD_PATH.value
        ], 
        wait_for=upload_finished
    )
    
    dumb_task(bucket=(BUCKET_NAME, ENVIRONMENT)) # Usada apenas no desenvolvimento
    
    