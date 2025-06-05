# -*- coding: utf-8 -*-
# pylint: disable=C0103
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.reports.checks_bucket_files.schedules import schedule
from pipelines.reports.checks_bucket_files.tasks import get_data

with Flow(
    name="Report: Monitoramento de arquivos no GCS"
) as report_bucket_files:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")
    BUCKET_NAME = 'cgcca_cnes'
    FILE_PREFIX = ''
    FILE_SUFFIX = '.GDB'
    
    
    #####################################
    # Tasks
    #####################################
    data = get_data(bucket_name=BUCKET_NAME, 
                    file_prefix=FILE_PREFIX, 
                    file_suffix=FILE_SUFFIX,
                    environment=ENVIRONMENT)


