# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.raw.vitacare.constants import constants as vitacare_constants
from pipelines.prontuarios.raw.vitacare.tasks import (
    extract_data_from_api,
    group_data_by_patient,
)
from pipelines.prontuarios.raw.vitai.tasks import get_entity_endpoint_name
from pipelines.prontuarios.utils.tasks import (
    force_garbage_collector,
    get_api_token,
    get_flow_scheduled_day,
    load_to_api,
    rename_current_flow_run,
    get_dates_in_range,
    transform_filter_valid_cpf,
    transform_to_raw_format,
    get_ap_from_cnes
)
from pipelines.utils.tasks import inject_gcp_credentials

with Flow(
    name="Prontuários (Vitacare) - Extração de Dados",
) as vitacare_extraction:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    CNES = Parameter("cnes", default="5621801", required=True)
    ENTITY = Parameter("entity", default="diagnostico", required=True)
    MIN_DATE = Parameter("minimum_date", default="", required=False)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################
    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)
    
    ap = get_ap_from_cnes(
        cnes=CNES,
        upstream_tasks=[credential_injection]
    )

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitacare_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitacare_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitacare_constants.INFISICAL_API_PASSWORD.value,
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
    maximum_date = get_flow_scheduled_day(upstream_tasks=[credential_injection])

    dates_of_interest = get_dates_in_range(
        minimum_date=MIN_DATE,
        maximum_date=maximum_date,
        upstream_tasks=[credential_injection],
    )

    daily_data_list = extract_data_from_api.map(
        cnes=unmapped(CNES),
        ap=unmapped(ap),
        target_day=dates_of_interest,
        entity_name=unmapped(ENTITY),
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

    ####################################
    # Task Section #3 - Prepare to Load
    ####################################
    request_bodies = transform_to_raw_format.map(
        json_data=valid_data,
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

#vitacare_extraction.schedule = vitacare_daily_update_schedule
vitacare_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitacare_extraction.executor = LocalDaskExecutor(num_workers=1)
vitacare_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="10Gi",
)
