# -*- coding: utf-8 -*-
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.std.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.std.smsrio.tasks import (
    clean_none_records,
    clean_records_fields,
    define_constants,
    drop_invalid_records,
    merge_keys,
    standardize_address_data,
    standardize_cns_list,
    standardize_decease_info,
    standardize_nationality,
    standardize_parents_names,
    standardize_race,
    standardize_telecom_data,
)
from pipelines.prontuarios.utils.tasks import get_api_token
from pipelines.utils.tasks import get_secret_key, inject_gcp_credentials, load_from_api

with Flow(
    name="Prontuários (SMSRio) - Padronização de Pacientes",
) as smsrio_standardization:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    START_DATETIME = Parameter("start_datetime", default="2024-02-06 12:00:00", required=False)
    END_DATETIME = Parameter("end_datetime", default="2024-02-06 12:04:00", required=False)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################
    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=smsrio_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=smsrio_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=smsrio_constants.INFISICAL_API_PASSWORD.value,
        upstream_tasks=[credential_injection],
    )

    api_url = get_secret_key(
        secret_path="/",
        secret_name=prontuarios_constants.INFISICAL_API_URL.value,
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection],
    )

    ####################################
    # Task Section #1 - Get Data
    ####################################
    raw_patient_data = load_from_api(
        url=api_url + "raw/patientrecords",
        params={
            "start_datetime": START_DATETIME,
            "end_datetime": END_DATETIME,
            "datasource_system": "smsrio",
        },
        credentials=api_token,
        auth_method="bearer",
    )

    ####################################
    # Task Section #2 - Transform Data
    ####################################

    lista_campos_api, logradouros_dict, city_dict, state_dict, country_dict = define_constants()

    patients_json_std = merge_keys.map(
        dic=raw_patient_data, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = drop_invalid_records.map(
        data=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = clean_none_records(
        json_list=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = standardize_race.map(
        data=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = standardize_nationality.map(
        data=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = standardize_parents_names.map(
        data=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = standardize_cns_list.map(
        data=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = standardize_decease_info.map(
        data=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = standardize_address_data.map(
        data=patients_json_std,
        logradouros_dict=unmapped(logradouros_dict),
        city_dict=unmapped(city_dict),
        state_dict=unmapped(state_dict),
        country_dict=unmapped(country_dict),
        upstream_tasks=[unmapped(credential_injection)],
    )
    patients_json_std = standardize_telecom_data.map(
        data=patients_json_std, upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std = clean_records_fields.map(
        data=patients_json_std,
        lista_campos_api=unmapped(lista_campos_api),
        upstream_tasks=[unmapped(credential_injection)],
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
