# -*- coding: utf-8 -*-
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.mrg.constants import constants as mrg_constants
from pipelines.prontuarios.mrg.schedules import mrg_daily_update_schedule
from pipelines.prontuarios.mrg.tasks import (
    flatten_page_data,
    get_mergeable_records_from_api,
    get_patient_count,
    merge,
    parse_date,
    put_to_api,
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_datetime_working_range,
    rename_current_flow_run,
    transform_create_input_batches,
)
from pipelines.utils.tasks import get_secret_key

with Flow(
    name="Prontuários - Unificação de Pacientes",
) as patientrecord_mrg:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    START_DATETIME = Parameter("start_datetime", default="today")
    END_DATETIME = Parameter("end_datetime", default="tomorrow")

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

    ####################################
    # Task Section #1 - Get Data
    ####################################
    parsed_start_datetime = parse_date(date=START_DATETIME)
    parsed_end_datetime = parse_date(date=END_DATETIME)

    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=parsed_start_datetime, end_datetime=parsed_end_datetime, return_as_str=True
    )

    mergeable_records_in_pages = get_mergeable_records_from_api(
        api_base_url=api_url,
        api_token=api_token,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
    )

    mergeable_records_flattened = flatten_page_data(data_in_pages=mergeable_records_in_pages)

    with case(RENAME_FLOW, True):
        patient_count = get_patient_count(data=mergeable_records_flattened)
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT, patient_count=patient_count
        )

    ####################################
    # Task Section #2 - Merge Data
    ####################################
    std_patient_list_final = merge(data_to_merge=mergeable_records_flattened)

    batches = transform_create_input_batches(
        input_list=std_patient_list_final,
        batch_size=1000,
    )

    put_to_api(
        payload_in_batch=batches,
        api_url=api_url,
        endpoint_name="mrg/patient",
        api_token=api_token,
    )


patientrecord_mrg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
patientrecord_mrg.executor = LocalDaskExecutor(num_workers=6)
patientrecord_mrg.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

patientrecord_mrg.schedule = mrg_daily_update_schedule
