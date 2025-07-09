# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
"""
medlab api flows
"""

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

from pipelines.constants import constants
from pipelines.datalake.extract_load.medlab_api.constants import medlab_api_constants
from pipelines.datalake.extract_load.medlab_api.schedules import medlab_api_schedule
from pipelines.datalake.extract_load.medlab_api.tasks import (
    get_exams_list_and_results,
    get_patient_code_from_bigquery,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import get_secret_key

with Flow(
    name="Medlab API - Pacientes",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DIT_ID.value],
) as flow_medlab_api:

    #####################################
    # Parameters
    #####################################

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    DT_START = Parameter("dt_start", default="2001-01-01")
    DT_END = Parameter("dt_end", default="2100-01-01")

    INFISICAL_PATH = medlab_api_constants.INFISICAL_PATH.value

    OUTPUT_DIRECTORY = "output"

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

    PATIENTS = get_patient_code_from_bigquery()

    exam_results = get_exams_list_and_results.map(
        api_url=unmapped(API_URL),
        api_usuario=unmapped(API_USUARIO),
        api_senha=unmapped(API_SENHA),
        api_codacesso=unmapped(API_CODACESSO),
        dt_start=unmapped(DT_START),
        dt_end=unmapped(DT_END),
        patientcode=PATIENTS,
        output_dir=unmapped(OUTPUT_DIRECTORY),
    )


flow_medlab_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_medlab_api.executor = LocalDaskExecutor(num_workers=5)
flow_medlab_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="8Gi",
    memory_request="8Gi",
)

flow_medlab_api.schedule = medlab_api_schedule
