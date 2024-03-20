# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.std.extracao.smsrio.tasks import (
    get_data_from_db,
    get_params,
    insert_data_to_db,
)
from pipelines.prontuarios.std.smsrio.tasks import (
    define_constants,
    format_json,
    standartize_data,
)
from pipelines.utils.tasks import inject_gcp_credentials

with Flow(
    name="Prontuários (SMSRio) - Padronização de carga histórica de pacientes",
) as smsrio_standardization_historical:
    #####################################
    # Parameters
    #####################################
    DATABASE = Parameter("database", required=True)
    USER = Parameter("user", required=True)
    PASSWORD = Parameter("password", required=True)
    IP = Parameter("ip", required=True)
    ENVIRONMENT = Parameter("environment", default="dev", required=True)

    START_DATETIME = Parameter(
        "source_start_datetime", default="2024-02-06 12:00:00", required=False
    )
    END_DATETIME = Parameter("source_end_datetime", default="2024-02-06 12:04:00", required=False)

    ####################################
    # Task Section #1 - Get Data
    ####################################

    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    request_params = get_params(START_DATETIME, END_DATETIME, upstream_tasks=[credential_injection])

    raw_patient_data = get_data_from_db(
        USER=USER,
        PASSWORD=PASSWORD,
        DATABASE=DATABASE,
        IP=IP,
        date_range=request_params,
        upstream_tasks=[credential_injection],
    )

    ####################################
    # Task Section #2 - Transform Data
    ####################################

    lista_campos_api, logradouros_dict, city_dict, state_dict, country_dict = define_constants(
        upstream_tasks=[credential_injection]
    )

    format_patient_list = format_json(raw_patient_data)

    std_patient_list = standartize_data(
        raw_data=format_patient_list,
        logradouros_dict=logradouros_dict,
        city_dict=city_dict,
        state_dict=state_dict,
        country_dict=country_dict,
        lista_campos_api=lista_campos_api,
        upstream_tasks=[credential_injection],
    )

    insert_data_to_db(
        USER=USER,
        PASSWORD=PASSWORD,
        IP=IP,
        DATABASE=DATABASE,
        data=std_patient_list,
        upstream_tasks=[credential_injection],
    )


smsrio_standardization_historical.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smsrio_standardization_historical.executor = LocalDaskExecutor(num_workers=4)
smsrio_standardization_historical.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="8Gi",
)
