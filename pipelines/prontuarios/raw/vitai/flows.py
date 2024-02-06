# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow for Vitai Raw Data Extraction
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.raw.vitai.constants import constants as vitai_constants
from pipelines.prontuarios.raw.vitai.schedules import vitai_daily_update_schedule
from pipelines.prontuarios.raw.vitai.tasks import (
    extract_data_from_api,
    get_vitai_api_token,
    group_cids_data_by_patient,
    group_patients_data_by_patient,
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

with Flow(
    name="Prontuários (Vitai) - Extração de Dados",
) as sms_prontuarios_raw_vitai:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")

    CNES = Parameter("cnes")

    RENAME_FLOW = Parameter("rename_flow", default=False)

    #####################################
    # Set environment
    ####################################
    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    vitai_api_token = get_vitai_api_token(environment="prod", upstream_tasks=[credential_injection])

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitai_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitai_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitai_constants.INFISICAL_API_PASSWORD.value,
        upstream_tasks=[credential_injection],
    )
    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT, cnes=CNES, upstream_tasks=[credential_injection]
        )

    ####################################
    # Task Section #1 - Get data
    ####################################
    target_day = get_flow_scheduled_day(upstream_tasks=[credential_injection])

    # Patient
    patients_data = extract_data_from_api(
        target_day=target_day,
        entity_name="pacientes",
        vitai_api_token=vitai_api_token,
        upstream_tasks=[credential_injection],
    )

    # CID
    cids_data = extract_data_from_api(
        target_day=target_day,
        entity_name="diagnostico",
        vitai_api_token=vitai_api_token,
        upstream_tasks=[credential_injection],
    )

    ####################################
    # Task Section #2 - Group data by CPF
    ####################################
    # Patient
    patient_data_grouped = group_patients_data_by_patient(
        patients_data=patients_data, upstream_tasks=[credential_injection]
    )
    valid_patients = transform_filter_valid_cpf(
        objects=patient_data_grouped, upstream_tasks=[credential_injection]
    )

    # CID
    cid_data_grouped = group_cids_data_by_patient(
        cids_data=cids_data, upstream_tasks=[credential_injection]
    )
    valid_cids = transform_filter_valid_cpf(
        objects=cid_data_grouped, upstream_tasks=[credential_injection]
    )

    ####################################
    # Task Section #3 - Prepare data to load
    ####################################
    # Patient
    valid_patients_batches = transform_create_input_batches(
        valid_patients, upstream_tasks=[credential_injection]
    )
    patients_request_bodies = transform_to_raw_format.map(
        json_data=valid_patients_batches,
        cnes=unmapped(CNES),
        upstream_tasks=[unmapped(credential_injection)],
    )

    # CID
    valid_cids_batches = transform_create_input_batches(
        valid_cids, upstream_tasks=[credential_injection]
    )
    cids_request_bodies = transform_to_raw_format.map(
        json_data=valid_cids_batches,
        cnes=unmapped(CNES),
        upstream_tasks=[unmapped(credential_injection)],
    )

    ####################################
    # Task Section #4 - Load to API
    ####################################
    # Patient
    load_to_api_task = load_to_api.map(
        request_body=patients_request_bodies,
        endpoint_name=unmapped("raw/patientrecords"),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
        upstream_tasks=[unmapped(credential_injection)],
    )

    # CID
    load_to_api_task = load_to_api.map(
        request_body=cids_request_bodies,
        endpoint_name=unmapped("raw/patientconditions"),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
        upstream_tasks=[unmapped(credential_injection)],
    )


sms_prontuarios_raw_vitai.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_raw_vitai.executor = LocalDaskExecutor(num_workers=10)
sms_prontuarios_raw_vitai.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

sms_prontuarios_raw_vitai.schedule = vitai_daily_update_schedule
