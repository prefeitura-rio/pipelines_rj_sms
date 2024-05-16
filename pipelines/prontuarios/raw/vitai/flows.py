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
    assert_api_availability,
    create_parameter_list,
    extract_data_from_api,
    get_dates_in_range,
    get_entity_endpoint_name,
    get_vitai_api_token,
    group_data_by_patient,
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_current_flow_labels,
    get_datetime_working_range,
    get_healthcenter_name_from_cnes,
    get_project_name,
    load_to_api,
    rename_current_flow_run,
    transform_filter_valid_cpf,
    transform_to_raw_format,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)

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
    START_DATETIME = Parameter("start_datetime", default="")
    END_DATETIME = Parameter("end_datetime", default="")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################
    vitai_api_token = get_vitai_api_token(environment="prod")

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitai_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitai_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitai_constants.INFISICAL_API_PASSWORD.value,
    )
    with case(RENAME_FLOW, True):
        healthcenter_name = get_healthcenter_name_from_cnes(cnes=CNES)

        rename_current_flow_run(
            environment=ENVIRONMENT, cnes=CNES, entity_type=ENTITY, unidade=healthcenter_name
        )

    ####################################
    # Task Section #1 - Get Data
    ####################################
    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=START_DATETIME,
        end_datetime=END_DATETIME
    )

    dates_of_interest = get_dates_in_range(
        minimum_date=start_datetime,
        maximum_date=end_datetime,
    )

    assertion_task = assert_api_availability(cnes=CNES, method="request")

    daily_data_list = extract_data_from_api.map(
        cnes=unmapped(CNES),
        target_day=dates_of_interest,
        entity_name=unmapped(ENTITY),
        vitai_api_token=unmapped(vitai_api_token),
        upstream_tasks=[unmapped(assertion_task)],
    )

    ####################################
    # Task Section #2 - Transform Data
    ####################################
    grouped_data = group_data_by_patient.map(
        data=daily_data_list,
        entity_type=unmapped(ENTITY),
    )
    valid_data = transform_filter_valid_cpf.map(objects=grouped_data)

    ####################################
    # Task Section #3 - Prepare to Load
    ####################################
    request_bodies = transform_to_raw_format.map(
        json_data=valid_data,
        cnes=unmapped(CNES),
    )

    endpoint_name = get_entity_endpoint_name(entity=ENTITY)

    ####################################
    # Task Section #4 - Load Data
    ####################################
    load_to_api_task = load_to_api.map(
        request_body=request_bodies,
        endpoint_name=unmapped(endpoint_name),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
    )

vitai_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_extraction.executor = LocalDaskExecutor(num_workers=5)
vitai_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="3Gi",
    memory_limit="3Gi",
)

# ==============================
# FLOW ORQUESTRADOR
# ==============================

with Flow(
    name="Prontuários (Vitai) - Agendador de Flows",
) as vitai_scheduler_flow:
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    START_DATETIME = Parameter("start_datetime", default="")
    END_DATETIME = Parameter("end_datetime", default="")

    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT)

    parameter_list = create_parameter_list(
        environment=ENVIRONMENT,
        start_datetime=START_DATETIME,
        end_datetime=END_DATETIME
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    current_flow_run_labels = get_current_flow_labels()

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("Prontuários (Vitai) - Extração de Dados"),
        project_name=unmapped(project_name),
        parameters=parameter_list,
        labels=unmapped(current_flow_run_labels),
    )

    wait_runs_task = wait_for_flow_run.map(
        flow_run_id=created_flow_runs,
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
)
