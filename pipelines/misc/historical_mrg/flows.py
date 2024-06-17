# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.misc.historical_mrg.tasks import (
    build_db_url,
    build_param_list,
    get_mergeable_data_from_db,
    have_already_executed,
    merge_patientrecords,
    save_progress,
)
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.mrg.constants import constants as mrg_constants
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_current_flow_labels,
    get_project_name,
    load_to_api,
    rename_current_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.tasks import get_secret_key

with Flow(
    name="Prontuários - Unificação de Pacientes (Histórico) - Batch",
) as mrg_historic_patientrecord_batch:
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    LIMIT = Parameter("limit", required=True)
    OFFSET = Parameter("offset", required=True)

    ####################################
    # Set environment
    ####################################
    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=mrg_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=mrg_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=mrg_constants.INFISICAL_API_PASSWORD.value,
    )

    api_url = get_secret_key(
        secret_path="/",
        secret_name=prontuarios_constants.INFISICAL_API_URL.value,
        environment=ENVIRONMENT,
    )

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT, is_initial_extraction=True, limit=LIMIT, offset=OFFSET
        )
    ####################################
    # Set environment
    ####################################
    db_url = build_db_url(environment=ENVIRONMENT)

    ####################################
    # Get data
    ####################################

    have_already_executed = have_already_executed(
        limit=LIMIT, offset=OFFSET, environment=ENVIRONMENT
    )

    with case(have_already_executed, False):
        mergeable_data = get_mergeable_data_from_db(limit=LIMIT, offset=OFFSET, db_url=db_url)

        ####################################
        # Merge
        ####################################
        patient_data, addresses_data, telecoms_data, cnss_data = merge_patientrecords(
            mergeable_data=mergeable_data
        )

        ####################################
        # Send Data to API
        ####################################
        patient_send_task = load_to_api(
            endpoint_name="mrg/patient",
            request_body=patient_data,
            api_token=api_token,
            api_url=api_url,
            environment=ENVIRONMENT,
            method="PUT",
        )
        address_send_task = load_to_api(
            endpoint_name="mrg/patientaddress",
            request_body=addresses_data,
            api_token=api_token,
            api_url=api_url,
            environment=ENVIRONMENT,
            method="PUT",
            upstream_tasks=[patient_send_task],
        )
        telecom_send_task = load_to_api(
            endpoint_name="mrg/patienttelecom",
            request_body=telecoms_data,
            api_token=api_token,
            api_url=api_url,
            environment=ENVIRONMENT,
            method="PUT",
            upstream_tasks=[patient_send_task],
        )
        cns_send_task = load_to_api(
            endpoint_name="mrg/patientcns",
            request_body=cnss_data,
            api_token=api_token,
            api_url=api_url,
            environment=ENVIRONMENT,
            method="PUT",
            upstream_tasks=[patient_send_task],
        )

        ####################################
        # Save Progress
        ####################################
        save_progress(
            limit=LIMIT,
            offset=OFFSET,
            environment=ENVIRONMENT,
            upstream_tasks=[patient_send_task, address_send_task, telecom_send_task, cns_send_task],
        )


mrg_historic_patientrecord_batch.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
mrg_historic_patientrecord_batch.executor = LocalDaskExecutor(num_workers=1)
mrg_historic_patientrecord_batch.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

with Flow(
    name="Prontuários - Unificação de Pacientes (Histórico)",
) as mrg_historic_patientrecord:
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    BATCH_SIZE = Parameter("batch_size", default=1000, required=True)

    db_url = build_db_url(environment=ENVIRONMENT)

    params = build_param_list(batch_size=BATCH_SIZE, db_url=db_url, environment=ENVIRONMENT)

    project_name = get_project_name(environment=ENVIRONMENT)

    current_flow_run_labels = get_current_flow_labels()

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("Prontuários - Unificação de Pacientes (Histórico) - Batch"),
        project_name=unmapped(project_name),
        parameters=params,
        labels=unmapped(current_flow_run_labels),
    )

    wait_runs_task = wait_for_flow_run.map(
        flow_run_id=created_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

mrg_historic_patientrecord.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
mrg_historic_patientrecord.executor = LocalDaskExecutor(num_workers=1)
mrg_historic_patientrecord.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)
