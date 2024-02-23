# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.std.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.std.smsrio.tasks import standardize_race
from pipelines.prontuarios.utils.tasks import get_api_token
from pipelines.utils.tasks import get_secret_key, inject_gcp_credentials, load_from_api

with Flow(
    name="Prontuários (SMSRio) - Padronização de Pacientes",
) as smsrio_standardization:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    START_DATETIME = Parameter("start_datetime", default="2024-02-06 12:00:00", required=False)
    END_DATETIME = Parameter("end_datetime", default="2024-02-06 12:04:00", required=False)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################
    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=smsrio_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=smsrio_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=smsrio_constants.INFISICAL_API_PASSWORD.value,
        upstream_tasks=[credential_injection],
    )

    api_url = get_secret_key(
        secret_path="/",
        secret_name=prontuarios_constants.INFISICAL_API_URL.value,
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection],
    )

    ####################################
    # Task Section #1 - Get Data
    ####################################
    raw_patient_data = load_from_api(
        url=api_url + "raw/patientrecords",
        params={"start_datetime": START_DATETIME, "end_datetime": END_DATETIME},
        credentials=api_token,
        auth_method="bearer",
    )

    ####################################
    # Task Section #2 - Transform Data
    ####################################
    race_stantardized = standardize_race(raw_patient_data)

smsrio_standardization.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smsrio_standardization.executor = LocalDaskExecutor(num_workers=4)
smsrio_standardization.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)
