# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
medilab api flows
"""

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.medilab_api.constants import medilab_api_constants
from pipelines.datalake.extract_load.medilab_api.tasks import (
    get_exams_list_and_results,
    get_patient_code_from_bigquery,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key

with Flow(
    name="DataLake - Extração e Carga de Dados - Medilab",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DIT_ID.value],
) as flow_medilab_api:

    #####################################
    # Parameters
    #####################################

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    DT_START = Parameter("dt_start", default="2001-01-01")
    DT_END = Parameter("dt_end", default="2100-01-01")
    DATE_FILTER = Parameter("date_filter", default="2025-07-01")

    INFISICAL_PATH = medilab_api_constants.INFISICAL_PATH.value

    OUTPUT_DIRECTORY = "output"
    BUCKET_NAME = medilab_api_constants.GCS_BUCKET_NAME.value

    API_URL = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name="API_URL", environment=ENVIRONMENT
    )

    API_USUARIO = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name="API_USUARIO", environment=ENVIRONMENT
    )

    API_SENHA = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name="API_SENHA", environment=ENVIRONMENT
    )

    API_CODACESSO = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name="API_CODACESSO", environment=ENVIRONMENT
    )

    PATIENTS = get_patient_code_from_bigquery(date_filter=DATE_FILTER)

    exam_results = get_exams_list_and_results.map(
        api_url=unmapped(API_URL),
        api_usuario=unmapped(API_USUARIO),
        api_senha=unmapped(API_SENHA),
        api_codacesso=unmapped(API_CODACESSO),
        dt_start=unmapped(DT_START),
        dt_end=unmapped(DT_END),
        patientcode=PATIENTS,
        output_dir=unmapped(OUTPUT_DIRECTORY),
        bucket_name=unmapped(BUCKET_NAME),
    )


flow_medilab_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_medilab_api.executor = LocalDaskExecutor(num_workers=5)
flow_medilab_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="8Gi",
    memory_request="8Gi",
)
