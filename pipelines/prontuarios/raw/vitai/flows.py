# -*- coding: utf-8 -*-
from prefect import Parameter, case, flatten, unmapped
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
    get_dates_in_range,
    get_entity_endpoint_name,
    get_vitai_api_token,
    group_data_by_patient,
)
from pipelines.prontuarios.utils.tasks import (
    force_garbage_collector,
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
) as vitai_extraction:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    CNES = Parameter("cnes", default="5717256", required=True)
    API_URL = Parameter("api_url", required=True)
    ENTITY = Parameter("entity", default="diagnostico", required=True)
    MIN_DATE = Parameter("minimum_date", default="", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
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
    # Task Section #1 - Get Data
    ####################################
    maximum_date = get_flow_scheduled_day()

    dates_of_interest = get_dates_in_range(
        minimum_date=MIN_DATE,
        maximum_date=maximum_date,
        upstream_tasks=[credential_injection],
    )

    daily_data_list = extract_data_from_api.map(
        target_day=dates_of_interest,
        entity_name=unmapped(ENTITY),
        url=unmapped(API_URL),
        vitai_api_token=unmapped(vitai_api_token),
        upstream_tasks=[unmapped(credential_injection)],
    )

    ####################################
    # Task Section #2 - Transform Data
    ####################################
    grouped_data = group_data_by_patient.map(
        data=daily_data_list,
        entity_type=unmapped(ENTITY),
        upstream_tasks=[unmapped(credential_injection)],
    )
    valid_data = transform_filter_valid_cpf.map(
        objects=grouped_data, upstream_tasks=[unmapped(credential_injection)]
    )

    all_valid_data = flatten(valid_data)

    ####################################
    # Task Section #3 - Prepare to Load
    ####################################
    valid_data_batches = transform_create_input_batches(
        input_list=all_valid_data,
        upstream_tasks=[credential_injection],
    )

    request_bodies = transform_to_raw_format.map(
        json_data=valid_data_batches,
        cnes=unmapped(CNES),
        upstream_tasks=[unmapped(credential_injection)],
    )

    endpoint_name = get_entity_endpoint_name(entity=ENTITY, upstream_tasks=[credential_injection])

    ####################################
    # Task Section #4 - Load Data
    ####################################
    load_to_api_task = load_to_api.map(
        request_body=request_bodies,
        endpoint_name=unmapped(endpoint_name),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
        upstream_tasks=[unmapped(credential_injection)],
    )

    force_garbage_collector(upstream_tasks=[load_to_api_task])


vitai_extraction.schedule = vitai_daily_update_schedule
vitai_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_extraction.executor = LocalDaskExecutor(num_workers=1)
vitai_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)
