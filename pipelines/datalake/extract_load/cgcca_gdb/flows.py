# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.cgcca_gdb.tasks import extract_cgcca_gdb

with Flow(
    name="DataLake - Extração e Carga de Dados - CGCCA GDB",
) as sms_dump_cgcca_gdb:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)

    extract_cgcca_gdb(environment=ENVIRONMENT)


# Storage and run configs
sms_dump_cgcca_gdb.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_cgcca_gdb.executor = LocalDaskExecutor(num_workers=10)
sms_dump_cgcca_gdb.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
