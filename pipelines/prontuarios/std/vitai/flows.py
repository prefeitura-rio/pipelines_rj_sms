# -*- coding: utf-8 -*-
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.std.vitai.constants import constants as vitai_constants
from pipelines.prontuarios.std.vitai.schedules import vitai_std_daily_update_schedule
from pipelines.prontuarios.std.vitai.tasks import (
    define_constants,
    format_json,
    get_params,
    standartize_data,
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_std_flow_scheduled_day,
    load_to_api,
)
from pipelines.utils.tasks import get_secret_key, inject_gcp_credentials, load_from_api

with Flow(
    name="Prontuários (Vitai) - Padronização de Pacientes",
) as vitai_standardization:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################
    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitai_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitai_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitai_constants.INFISICAL_API_PASSWORD.value,
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
    START_DATETIME = get_std_flow_scheduled_day(upstream_tasks=[credential_injection])
    request_params = get_params(START_DATETIME, upstream_tasks=[credential_injection])

    raw_patient_data = load_from_api(
        url=api_url + "raw/patientrecords",
        params=request_params,
        credentials=api_token,
        auth_method="bearer",
    )

    ####################################
    # Task Section #2 - Transform Data
    ####################################

    city_name_dict, state_dict, country_dict = define_constants(
        upstream_tasks=[credential_injection]
    )

    format_patient_list = format_json(
        raw_patient_data, upstream_tasks=[unmapped(credential_injection)]
    )

    std_patient_list = standartize_data(
        raw_data=format_patient_list,
        city_name_dict=city_name_dict,
        state_dict=state_dict,
        country_dict=country_dict,
        upstream_tasks=[unmapped(credential_injection)],
    )

    load_to_api_task = load_to_api(
        upstream_tasks=[credential_injection],
        request_body=std_patient_list,
        endpoint_name="std/patientrecords",
        api_token=api_token,
        environment=ENVIRONMENT,
    )


vitai_standardization.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_standardization.executor = LocalDaskExecutor(num_workers=4)
vitai_standardization.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)


vitai_standardization.schedule = vitai_std_daily_update_schedule
