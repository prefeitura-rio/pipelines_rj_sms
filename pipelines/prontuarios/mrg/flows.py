# -*- coding: utf-8 -*-
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.mrg.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.mrg.smsrio.schedules import smsrio_std_daily_update_schedule
from pipelines.prontuarios.mrg.smsrio.tasks import (
    define_constants,
    format_json,
    get_params,
    standartize_data,
)
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    get_mrg_flow_scheduled_day,
    load_to_api,
    rename_current_mrg_flow_run,
)
from pipelines.utils.tasks import get_secret_key, load_from_api

with Flow(
    name="Prontuários - Fundição de Pacientes ",
) as smsrio_standardization:
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
        infisical_path=smsrio_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=smsrio_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=smsrio_constants.INFISICAL_API_PASSWORD.value,
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
    # Task Section #2 - Transform Data
    ####################################

    std_patient_list = merge(
        std_data=std_data_to_merge
    )

    load_to_api_task = load_to_api(
        request_body=std_patient_list,
        endpoint_name="mrg/patient",
        api_token=api_token,
        environment=ENVIRONMENT,
    )


smsrio_standardization.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smsrio_standardization.executor = LocalDaskExecutor(num_workers=4)
smsrio_standardization.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

smsrio_standardization.schedule = smsrio_std_daily_update_schedule
