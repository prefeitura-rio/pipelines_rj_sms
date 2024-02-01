# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow for SMSRio Raw Data Extraction
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.raw.smsrio.schedules import smsrio_daily_update_schedule
from pipelines.prontuarios.raw.smsrio.tasks import (
    extract_patient_data_from_db,
    get_database_url,
    transform_data_to_json,
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_flow_scheduled_day,
    load_to_api,
    rename_current_flow_run,
    transform_create_input_batches,
    transform_filter_valid_cpf,
    transform_to_raw_format,
)
from pipelines.utils.tasks import inject_gcp_credentials

####################################
# Daily Routine Flow
####################################
with Flow(
    name="Prontuários (SMSRio) - Extração de Dados",
) as sms_prontuarios_raw_smsrio:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")

    IS_INITIAL_EXTRACTION = Parameter("is_initial_extraction", default=False)

    RENAME_FLOW = Parameter("rename_flow", default=False)

    #####################################
    # Set environment
    ####################################
    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    database_url = get_database_url(environment=ENVIRONMENT, upstream_tasks=[credential_injection])

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=smsrio_constants.INFISICAL_PATH.value,
        infisical_api_username=smsrio_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=smsrio_constants.INFISICAL_API_PASSWORD.value,
        upstream_tasks=[credential_injection],
    )

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT,
            cnes=smsrio_constants.SMSRIO_CNES.value,
            is_initial_extraction=IS_INITIAL_EXTRACTION,
            upstream_tasks=[credential_injection],
        )

    ####################################
    # Task Section #1 - Get data
    ####################################
    with case(IS_INITIAL_EXTRACTION, True):
        patient_data = extract_patient_data_from_db(
            db_url=database_url, upstream_tasks=[credential_injection]
        )

    with case(IS_INITIAL_EXTRACTION, False):
        target_day = get_flow_scheduled_day(upstream_tasks=[credential_injection])

        patient_data = extract_patient_data_from_db(
            db_url=database_url,
            time_window_start=target_day,
            time_window_duration=1,
            upstream_tasks=[credential_injection],
        )

    ####################################
    # Task Section #2 - Prepare data to load
    ####################################
    json_list = transform_data_to_json(
        dataframe=patient_data,
        identifier_column="patient_cpf",
        upstream_tasks=[credential_injection],
    )

    valid_patients = transform_filter_valid_cpf(
        objects=json_list, upstream_tasks=[credential_injection]
    )

    json_list_batches = transform_create_input_batches(
        valid_patients, upstream_tasks=[credential_injection]
    )

    request_bodies = transform_to_raw_format.map(
        json_data=json_list_batches,
        cnes=unmapped(smsrio_constants.SMSRIO_CNES.value),
        upstream_tasks=[unmapped(credential_injection)],
    )

    ####################################
    # Task Section #3 - Loading data
    ####################################
    load_to_api_task = load_to_api.map(
        request_body=request_bodies,
        endpoint_name=unmapped("raw/patientrecords"),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
        upstream_tasks=[unmapped(credential_injection)],
    )


sms_prontuarios_raw_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_raw_smsrio.executor = LocalDaskExecutor(num_workers=10)
sms_prontuarios_raw_smsrio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)

sms_prontuarios_raw_smsrio.schedule = smsrio_daily_update_schedule
