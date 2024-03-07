# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.raw.vitai.constants import constants as vitai_constants
from pipelines.prontuarios.raw.vitai.schedules import vitai_daily_update_schedule
from pipelines.prontuarios.raw.vitai.tasks import (
    create_parameter_list,
    extract_data_from_api,
    get_dates_in_range,
    get_entity_endpoint_name,
    get_vitai_api_token,
    group_data_by_patient,
)
from pipelines.prontuarios.utils.tasks import (
    force_garbage_collector,
    get_api_token,
    get_current_flow_labels,
    get_flow_scheduled_day,
    get_project_name,
    load_to_api,
    rename_current_flow_run,
    transform_filter_valid_cpf,
    transform_to_raw_format,
)
from pipelines.utils.tasks import inject_gcp_credentials

# ==============================
# FLOW DE EXTRAÇÃO DE DADOS
# ==============================

with Flow(
    name="Prontuários (Vitai) - Extração de Dados",
) as vitai_extraction:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    CNES = Parameter("cnes", default="5717256", required=True)
    ENTITY = Parameter("entity", default="diagnostico", required=True)
    MIN_DATE = Parameter("minimum_date", default="", required=False)
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
    maximum_date = get_flow_scheduled_day(upstream_tasks=[credential_injection])

    dates_of_interest = get_dates_in_range(
        minimum_date=MIN_DATE,
        maximum_date=maximum_date,
        upstream_tasks=[credential_injection],
    )

    daily_data_list = extract_data_from_api.map(
        cnes=unmapped(CNES),
        target_day=dates_of_interest,
        entity_name=unmapped(ENTITY),
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

vitai_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_extraction.executor = LocalDaskExecutor(num_workers=5)
vitai_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="10Gi",
)

# ==============================
# FLOW ORQUESTRADOR
# ==============================

with Flow(
    name="Prontuários (Vitai) - Scheduler Flow",
) as vitai_scheduler_flow:
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    MIN_DATE = Parameter("minimum_date", default="", required=False)

    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            upstream_tasks=[credential_injection],
        )

    parameter_list = create_parameter_list(
        environment=ENVIRONMENT,
        minimum_date=MIN_DATE,
        upstream_tasks=[credential_injection],
    )

    project_name = get_project_name(
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection],
    )

    current_flow_run_labels = get_current_flow_labels(upstream_tasks=[credential_injection])

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("Prontuários (Vitai) - Extração de Dados"),
        project_name=unmapped(project_name),
        parameters=parameter_list,
        labels=unmapped(current_flow_run_labels),
        upstream_tasks=[unmapped(credential_injection)],
    )

    wait_runs_task = wait_for_flow_run.map(
        created_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

vitai_scheduler_flow.schedule = vitai_daily_update_schedule
vitai_scheduler_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_scheduler_flow.executor = LocalDaskExecutor(num_workers=1)
vitai_scheduler_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="1Gi",
)
