# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
    authenticated_wait_for_flow_run as wait_for_flow_run
)
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.raw.vitacare.constants import constants as vitacare_constants
from pipelines.prontuarios.raw.vitacare.schedules import vitacare_daily_update_schedule
from pipelines.prontuarios.raw.vitacare.tasks import (
    create_parameter_list,
    extract_data_from_api,
    extract_data_from_dump,
    group_data_by_patient,
)
from pipelines.prontuarios.raw.vitai.tasks import get_entity_endpoint_name
from pipelines.prontuarios.utils.tasks import (
    force_garbage_collector,
    get_ap_from_cnes,
    get_api_token,
    get_current_flow_labels,
    get_healthcenter_name_from_cnes,
    get_project_name,
    load_to_api,
    rename_current_flow_run,
    transform_create_input_batches,
    transform_filter_valid_cpf,
    transform_to_raw_format,
)

with Flow(
    name="Prontuários (Vitacare) - Extração de Dados",
) as vitacare_extraction:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    CNES = Parameter("cnes", default="5621801", required=True)
    ENTITY = Parameter("entity", default="diagnostico", required=True)
    TARGET_DATE = Parameter("target_date", default="", required=False)
    INITIAL_EXTRACTION = Parameter("initial_extraction", default=False)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################
    ap = get_ap_from_cnes(cnes=CNES)

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitacare_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitacare_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitacare_constants.INFISICAL_API_PASSWORD.value,
    )

    with case(RENAME_FLOW, True):
        healthcenter_name = get_healthcenter_name_from_cnes(cnes=CNES)

        rename_current_flow_run(
            environment=ENVIRONMENT,
            unidade=healthcenter_name,
            entity_type=ENTITY,
        )

    ####################################
    # Task Section #1 - Get Data
    ####################################
    with case(INITIAL_EXTRACTION, True):
        chunked_dump_data = extract_data_from_dump(
            cnes=CNES,
            ap=ap,
            entity_name=ENTITY,
            environment=ENVIRONMENT,
        )

    with case(INITIAL_EXTRACTION, False):
        api_data = extract_data_from_api(
            cnes=CNES,
            ap=ap,
            target_day=TARGET_DATE,
            entity_name=ENTITY,
            environment=ENVIRONMENT,
        )

        chunked_api_data = transform_create_input_batches(input_list=api_data, batch_size=1000)

    chunked_data = merge(chunked_dump_data, chunked_api_data)

    ####################################
    # Task Section #2 - Transform Data
    ####################################
    grouped_data = group_data_by_patient.map(
        data=chunked_data,
        entity_type=unmapped(ENTITY),
    )
    valid_data = transform_filter_valid_cpf.map(objects=grouped_data)

    ####################################
    # Task Section #3 - Prepare to Load
    ####################################
    request_bodies = transform_to_raw_format.map(json_data=valid_data, cnes=unmapped(CNES))

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

    force_garbage_collector(upstream_tasks=[load_to_api_task])

vitacare_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitacare_extraction.executor = LocalDaskExecutor(num_workers=5)
vitacare_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="8Gi",
    memory_limit="8Gi",
)


# ==============================
# SCHEDULER FLOW
# ==============================

with Flow(
    name="Prontuários (Vitacare) - Agendador de Flows",
) as vitacare_scheduler_flow:
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    INITIAL_EXTRACTION = Parameter("initial_extraction", default=False)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
        )

    parameter_list = create_parameter_list(
        environment=ENVIRONMENT,
        initial_extraction=INITIAL_EXTRACTION,
    )

    project_name = get_project_name(
        environment=ENVIRONMENT,
    )

    current_flow_run_labels = get_current_flow_labels()

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("Prontuários (Vitacare) - Extração de Dados"),
        project_name=unmapped(project_name),
        parameters=parameter_list,
        labels=unmapped(current_flow_run_labels),
    )

    wait_runs_task = wait_for_flow_run.map(
        created_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

vitacare_scheduler_flow.schedule = vitacare_daily_update_schedule
vitacare_scheduler_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitacare_scheduler_flow.executor = LocalDaskExecutor(num_workers=1)
vitacare_scheduler_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)
