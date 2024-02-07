# -*- coding: utf-8 -*-
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
    get_entity_endpoint_name,
    get_vitai_api_token,
    group_data_by_patient,
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
) as vitai_routine_extraction:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)

    CNES = Parameter("cnes", default="5717256", required=True)

    ENTITY = Parameter("entity", default="diagnostico", required=True)

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
        rename_current_flow_run(
            environment=ENVIRONMENT,
            cnes=CNES,
            entity_type=ENTITY,
            upstream_tasks=[credential_injection],
        )

    ####################################
    # Task Section #1 - Get data
    ####################################
    target_day = get_flow_scheduled_day(upstream_tasks=[credential_injection])

    data = extract_data_from_api(
        target_day=target_day,
        entity_name=ENTITY,
        vitai_api_token=vitai_api_token,
        upstream_tasks=[credential_injection],
    )

    ####################################
    # Task Section #2 - Group data by CPF
    ####################################
    grouped_data = group_data_by_patient(
        data=data, entity_type=ENTITY, upstream_tasks=[credential_injection]
    )
    valid_data = transform_filter_valid_cpf(
        objects=grouped_data, upstream_tasks=[credential_injection]
    )

    ####################################
    # Task Section #3 - Prepare data to load
    ####################################
    valid_data_batches = transform_create_input_batches(
        valid_data, upstream_tasks=[credential_injection]
    )
    request_bodies = transform_to_raw_format.map(
        json_data=valid_data_batches,
        cnes=unmapped(CNES),
        upstream_tasks=[unmapped(credential_injection)],
    )

    ####################################
    # Task Section #4 - Load to API
    ####################################
    endpoint_name = get_entity_endpoint_name(entity=ENTITY, upstream_tasks=[credential_injection])

    load_to_api.map(
        request_body=request_bodies,
        endpoint_name=unmapped(endpoint_name),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
        upstream_tasks=[unmapped(credential_injection)],
    )

####################################
# CONFIGURATION
####################################

vitai_routine_extraction.schedule = vitai_daily_update_schedule
vitai_routine_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_routine_extraction.executor = LocalDaskExecutor(num_workers=5)
vitai_routine_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
