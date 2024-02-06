# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow for SMSRio Raw Data Extraction
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.raw.smsrio.schedules import smsrio_daily_update_schedule
from pipelines.prontuarios.raw.smsrio.tasks import (
    extract_patient_data_from_db,
    get_smsrio_database_url,
    load_patient_data_to_api,
    transform_filter_invalid_cpf,
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_flow_scheduled_day,
    rename_current_flow_run,
    transform_split_dataframe,
)
from pipelines.utils.tasks import inject_gcp_credentials, load_file_from_gcs_bucket

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

    database_url = get_smsrio_database_url(
        environment=ENVIRONMENT, upstream_tasks=[credential_injection]
    )

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=smsrio_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
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
        patient_data_gcs = load_file_from_gcs_bucket(
            bucket_name=smsrio_constants.SMSRIO_BUCKET.value,
            file_name=smsrio_constants.SMSRIO_FILE_NAME.value,
            upstream_tasks=[credential_injection],
        )

    with case(IS_INITIAL_EXTRACTION, False):
        target_day = get_flow_scheduled_day(upstream_tasks=[credential_injection])

        patient_data_db = extract_patient_data_from_db(
            db_url=database_url,
            time_window_start=target_day,
            time_window_duration=1,
            upstream_tasks=[credential_injection],
        )

    patient_data = merge(patient_data_gcs, patient_data_db)

    ####################################
    # Task Section #2 - Prepare data to load
    ####################################
    patient_valid_data = transform_filter_invalid_cpf(
        dataframe=patient_data, cpf_column="patient_cpf", upstream_tasks=[credential_injection]
    )

    patient_data_batches = transform_split_dataframe(
        dataframe=patient_valid_data, batch_size=500, upstream_tasks=[credential_injection]
    )

    ####################################
    # Task Section #3 - Loading data
    ####################################
    load_patient_data_to_api.map(
        patient_data=patient_data_batches,
        environment=unmapped(ENVIRONMENT),
        api_token=unmapped(api_token),
        upstream_tasks=[unmapped(credential_injection)],
    )


sms_prontuarios_raw_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_raw_smsrio.executor = LocalDaskExecutor(num_workers=5)
sms_prontuarios_raw_smsrio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="8Gi",
    memory_limit="13.93Gi",
)

sms_prontuarios_raw_smsrio.schedule = smsrio_daily_update_schedule
