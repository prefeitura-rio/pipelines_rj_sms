# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants


with Flow(
    name="DataLake - Extração e Carga de Dados - Vitacare GDrive",
) as sms_dump_vitacare_reports:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW_RUN = Parameter("rename_flow_run", default=False, required=False)

    

# Storage and run configs
sms_dump_vitacare_reports.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_reports.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare_reports.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
