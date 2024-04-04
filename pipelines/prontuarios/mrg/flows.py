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
    get_params,
    normalize_payload_list,
    first_merge,
    final_merge
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_mrg_flow_scheduled_day,
    load_to_api,
    rename_current_mrg_flow_run,
)
from pipelines.utils.tasks import get_secret_key, load_from_api

with Flow(
    name="Prontuários - Fundição de Pacientes",
) as patientrecord_mrg:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_mrg_flow_run(environment=ENVIRONMENT)

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
    START_DATETIME = get_mrg_flow_scheduled_day()
    request_params = get_params(start_datetime=START_DATETIME)

    data_to_be_merged = load_from_api( # trazer TODOS OS REGISTROS dos patient_code que tiveram novos registros padronizados
        url=api_url + "std/patientrecords/fromInsertionDatetime",
        params=request_params,
        credentials=api_token,
        auth_method="bearer",
    )

    ####################################
    # Task Section #2 - Merge Data
    ####################################

    std_patient_list = normalize_payload_list.map(
        std_data=data_to_be_merged
    )

    merged_list, not_merged_list = first_merge.map(
        group=std_patient_list
    )

    std_patient_list_final = final_merge.map(
        merged_payload=merged_list,
        not_merged_payload=not_merged_list
    )

    load_to_api_task = load_to_api(
        request_body=std_patient_list,
        endpoint_name="mrg/patient",
        api_token=api_token,
        environment=ENVIRONMENT,
    )


patientrecord_mrg.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
patientrecord_mrg.executor = LocalDaskExecutor(num_workers=4)
patientrecord_mrg.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

patientrecord_mrg.schedule = mrg_daily_update_schedule
