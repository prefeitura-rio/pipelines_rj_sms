# -*- coding: utf-8 -*-
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.mrg.constants import constants as mrg_constants
from pipelines.prontuarios.mrg.schedules import mrg_daily_update_schedule
from pipelines.prontuarios.mrg.tasks import (
    get_params,
    load_mergeable_data,
    merge,
    print_n_patients,
    put_to_api,
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    transform_create_input_batches,
)
from pipelines.utils.tasks import get_secret_key, load_from_api

with Flow(
    name="Prontuários - Unificação de Pacientes",
) as patientrecord_mrg:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    START_DATETIME = Parameter("START_DATETIME", default="2024-03-12 14:41")
    END_DATETIME = Parameter("END_DATETIME", default="2024-03-12 14:42")

    ####################################
    # Set environment
    ####################################

    # with case(RENAME_FLOW, True):
    #     rename_flow_task = rename_current_mrg_flow_run(environment=ENVIRONMENT)

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

    ####################################
    # Task Section #1 - Get Data
    ####################################
    # START_DATETIME = get_mrg_flow_scheduled_day()
    request_params = get_params(start_datetime=START_DATETIME, end_datetime=END_DATETIME)

    cpfs_w_new_data = load_from_api(
        url=api_url + "std/patientrecords/updated",
        params=request_params,
        credentials=api_token,
        auth_method="bearer",
    )

    cpfs_w_new_data_printed = print_n_patients(data=cpfs_w_new_data)

    cpfs_w_new_data_batches = transform_create_input_batches(
        input_list=cpfs_w_new_data_printed, batch_size=1000
    )

    data_to_be_merged = load_mergeable_data.map(
        url=unmapped(api_url + "std/patientrecords"),
        cpfs=cpfs_w_new_data_batches,
        credentials=unmapped(api_token),
    )

    ####################################
    # Task Section #2 - Merge Data
    ####################################

    std_patient_list_final = merge(data_to_merge=data_to_be_merged)

    std_patient_batches_final = transform_create_input_batches(
        input_list=std_patient_list_final, batch_size=1000
    )

    put_to_api.map(
        request_body=std_patient_batches_final,
        api_url=unmapped(api_url),
        endpoint_name=unmapped("mrg/patient"),
        api_token=unmapped(api_token),
    )


patientrecord_mrg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
patientrecord_mrg.executor = LocalDaskExecutor(num_workers=2)
patientrecord_mrg.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

patientrecord_mrg.schedule = mrg_daily_update_schedule
