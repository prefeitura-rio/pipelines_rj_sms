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
    get_params,
    define_constants,
    merge_keys,
    drop_invalid_records,
    clean_none_records,
    standardize_nationality,
    standardize_race,
    standardize_decease_info,
    standardize_address_data,
    standardize_parents_names,
    standardize_cns_list,
    standardize_telecom_data,
    clean_records_fields
    )
from pipelines.prontuarios.utils.tasks import load_to_api


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
    request_params = get_params(
        START_DATETIME,
        END_DATETIME,
        upstream_tasks=[credential_injection]
        )

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
                        dic=raw_patient_data,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_valid_records = drop_invalid_records.map(
                        data=patients_json_std,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_notna_records = clean_none_records(
                        json_list=patients_json_valid_records,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_race = standardize_race.map(
                        data=patients_json_notna_records,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_nationality = standardize_nationality.map(
                        data=patients_json_std_race,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_parents = standardize_parents_names.map(
                        data=patients_json_std_nationality,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_cns = standardize_cns_list.map(
                        data=patients_json_std_parents,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_decease = standardize_decease_info.map(
                        data=patients_json_std_cns,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_address = standardize_address_data.map(
                        data=patients_json_std_decease,
                        logradouros_dict=unmapped(logradouros_dict),
                        city_dict=unmapped(city_dict),
                        state_dict=unmapped(state_dict),
                        country_dict=unmapped(country_dict),
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_telecom = standardize_telecom_data.map(
                        data=patients_json_std_address,
                        upstream_tasks=[unmapped(credential_injection)]
    )
    patients_json_std_clean = clean_records_fields.map(
                        data=patients_json_std_telecom,
                        lista_campos_api=unmapped(lista_campos_api),
                        upstream_tasks=[unmapped(credential_injection)]
    )
    load_to_api_task = load_to_api(
        upstream_tasks=[credential_injection],
        request_body=patients_json_std_clean,
        endpoint_name="std/patientrecords",
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
