# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.std.vitai.constants import constants as vitai_constants
from pipelines.prontuarios.std.vitai.schedules import vitai_std_daily_update_schedule
from pipelines.prontuarios.std.vitai.tasks import (
    define_constants,
    format_json,
    get_params,
    standartize_data
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_datetime_working_range,
    load_to_api,
    rename_current_std_flow_run,
    transform_create_input_batches,
)
from pipelines.utils.tasks import get_secret_key, load_from_api

with Flow(
    name="Prontuários (Vitai) - Padronização de Pacientes",
) as vitai_standardization:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    START_DATETIME = Parameter("start_datetime", default="")
    END_DATETIME = Parameter("end_datetime", default="")

    ####################################
    # Set environment
    ####################################

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_std_flow_run(environment=ENVIRONMENT, unidade="Vitai")

    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=START_DATETIME,
        end_datetime=END_DATETIME,
    )

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=vitai_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=vitai_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=vitai_constants.INFISICAL_API_PASSWORD.value,
    )

    api_url = get_secret_key(
        secret_path="/",
        secret_name=prontuarios_constants.INFISICAL_API_URL.value,
        environment=ENVIRONMENT,
    )

    ####################################
    # Task Section #1 - Get Data
    ####################################
    request_params = get_params(start_datetime=start_datetime, end_datetime=end_datetime)

    raw_patient_data = load_from_api(
        url=api_url + "raw/patientrecords/fromInsertionDatetime",
        params=request_params,
        credentials=api_token,
        auth_method="bearer",
    )

    ####################################
    # Task Section #2 - Transform Data
    ####################################

    city_name_dict, state_dict, country_dict = define_constants()

    json_list_valid, list_invalid_id = format_json(json_list=raw_patient_data)

    load_to_api_invalid_ids = load_to_api(
        request_body=list_invalid_id,
        endpoint_name="raw/patientrecords/setAsInvalid",
        api_token=api_token,
        environment=ENVIRONMENT,
    )

    std_patient_list = standartize_data(
        raw_data=json_list_valid,
        city_name_dict=city_name_dict,
        state_dict=state_dict,
        country_dict=country_dict,
    )

    std_patient_list_batches = transform_create_input_batches(
        input_list=std_patient_list, batch_size=1000
    )
    
    load_to_api_task = load_to_api.map(
        request_body=std_patient_list_batches,
        endpoint_name=unmapped("std/patientrecords"),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
    )


vitai_standardization.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
vitai_standardization.executor = LocalDaskExecutor(num_workers=4)
vitai_standardization.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)


vitai_standardization.schedule = vitai_std_daily_update_schedule
