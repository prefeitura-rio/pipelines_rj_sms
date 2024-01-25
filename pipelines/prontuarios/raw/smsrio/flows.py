# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow for SMSRio Raw Data Extraction
"""
from prefect import case, unmapped
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.executors import LocalDaskExecutor
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import constants
from pipelines.utils.tasks import (
    inject_gcp_credentials,
)
from pipelines.prontuarios.raw.smsrio.tasks import (
    get_database_url,
    extract_patient_data_from_db,
    extract_cns_data_from_db,
    transform_merge_patient_and_cns_data,
    transform_data_to_json,
)
from pipelines.prontuarios.utils.tasks import (
    get_flow_scheduled_day,
    get_api_token,
    transform_create_input_batches,
    transform_to_raw_format,
    transform_filter_valid_cpf,
    load_to_api,
    rename_current_flow_run
)
from pipelines.prontuarios.raw.smsrio.schedules import (
    smsrio_daily_update_schedule
)
from pipelines.prontuarios.raw.smsrio.constants import (
    constants as smsrio_constants
)


with Flow(
    name="Prontuários (SMSRio)- Extração de Dados",
) as sms_prontuarios_raw_smsrio:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")

    RENAME_FLOW = Parameter("rename_flow", default=False)

    #####################################
    # Set environment
    ####################################
    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    database_url = get_database_url(
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection]
    )

    api_token = get_api_token(
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection]
    )

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT,
            cnes=smsrio_constants.SMSRIO_CNES.value,
            upstream_tasks=[credential_injection]
        )

    ####################################
    # Task Section #1 - Get data
    ####################################
    target_day = get_flow_scheduled_day(
        upstream_tasks=[credential_injection]
    )

    patient_data = extract_patient_data_from_db(
        db_url=database_url,
        time_window_start=target_day,
        time_window_duration=1,
        upstream_tasks=[credential_injection]
    )

    cns_data = extract_cns_data_from_db(
        db_url=database_url,
        time_window_start=target_day,
        time_window_duration=1,
        upstream_tasks=[credential_injection]
    )

    ####################################
    # Task Section #2 - Transform and merge data
    ####################################
    merged_patient_data = transform_merge_patient_and_cns_data(
        patient_data=patient_data,
        cns_data=cns_data,
        upstream_tasks=[credential_injection]
    )

    ####################################
    # Task Section #3 - Prepare data to load
    ####################################
    json_list = transform_data_to_json(
        dataframe=merged_patient_data,
        identifier_column="patient_cpf",
        upstream_tasks=[credential_injection]
    )

    valid_patients = transform_filter_valid_cpf(
        objects=json_list,
        upstream_tasks=[credential_injection]
    )

    json_list_batches = transform_create_input_batches(
        valid_patients,
        upstream_tasks=[credential_injection]
    )

    request_bodies = transform_to_raw_format.map(
        json_data=json_list_batches,
        cnes=unmapped(smsrio_constants.SMSRIO_CNES.value),
        upstream_tasks=[unmapped(credential_injection)]
    )

    ####################################
    # Task Section #4 - Loading data
    ####################################
    load_to_api_task = load_to_api.map(
        request_body=request_bodies,
        endpoint_name=unmapped("raw/patientrecords"),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
        upstream_tasks=[unmapped(credential_injection)]
    )


sms_prontuarios_raw_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_raw_smsrio.executor = LocalDaskExecutor(num_workers=10)
sms_prontuarios_raw_smsrio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi"
)

sms_prontuarios_raw_smsrio.schedule = smsrio_daily_update_schedule
