# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.prontuario_cgs.constants import constants as prontuario_constants
from pipelines.datalake.extract_load.prontuario_cgs.tasks import (
    get_file,
    unpack_files,
    print_log
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name='DataLake - Extração e Carga de Dados - ProntuaRIO Backups',
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value]
) as prontuario_extraction:
    
    ENVIRONMENT = Parameter('environment', default='dev', required=True)
    BUCKET_NAME = 'subhue_backups'
    BLOB_PREFIX = '2025-11'

    downloaded_files = get_file(
        path=prontuario_constants.DOWNLOAD_DIR.value,
        bucket_name=BUCKET_NAME,
        environment=ENVIRONMENT,
        blob_prefix=BLOB_PREFIX
    )
    
    output = unpack_files(
        tar_file=downloaded_files,
        output_dir=prontuario_constants.UNCOMPRESS_FILES_DIR.value
    )
    
    print_log(message=output)