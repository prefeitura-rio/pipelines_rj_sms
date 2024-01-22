# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Raw Data Extraction and Load
"""
from prefect import unmapped
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.executors import LocalDaskExecutor

from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import task_rename_current_flow_run_dataset_table

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuario_constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants

from pipelines.utils.tasks import (
    inject_gcp_credentials,
    get_secret_key
)
from pipelines.prontuarios.raw.smsrio.tasks import (
    get_scheduled_window,
    get_api_token,
    get_api_url,
    extract_tabledata_from_db,
    transform_merge_patient_and_cns_data,
    transform_standardize_columns_names,
    transform_data_to_json,
    transform_create_input_batches,
    load_to_raw_endpoint,
)
from pipelines.prontuarios.raw.smsrio.schedules import (
    smsrio_daily_update_schedule
)


with Flow(
    name="Prontuários - Extração de Dados SMSRio",
) as sms_prontuarios_raw_smsrio:
    #####################################
    # Parameters
    #####################################

    ENVIRONMENT = Parameter("environment", default="dev")
    DATE = Parameter("date", default=None)

    #####################################
    # Set environment
    ####################################
    database_url = get_secret_key(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_DB_URL.value,
        environment=ENVIRONMENT
    )
    api_url = get_api_url(
        environment=ENVIRONMENT
    )
    api_username = get_secret_key(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_API_USERNAME.value,
        environment=ENVIRONMENT
    )
    api_password = get_secret_key(
        secret_path=smsrio_constants.SMSRIO_INFISICAL_PATH.value,
        secret_name=smsrio_constants.SMSRIO_INFISICAL_API_PASSWORD.value,
        environment=ENVIRONMENT
    )
    api_token = get_api_token(api_url, api_username, api_password)

    #####################################
    # Task
    ####################################
    window_start, window_end = get_scheduled_window()
        
    patient_data = extract_tabledata_from_db(
        db_url              = database_url,
        tablename           = "tb_pacientes",
        min_date            = window_start,
        max_date            = window_end,
        date_lookup_field   = "timestamp",
    )

    cns_data = extract_tabledata_from_db(
        db_url              = database_url,
        tablename           = "tb_cns_provisorios",
        min_date            = window_start,
        max_date            = window_end,
        date_lookup_field   = "timestamp",
    )

    patient_data = transform_standardize_columns_names(
        dataframe   = patient_data,
        columns_map = {'cpf': 'patient_cpf'}
    )

    cns_data = transform_standardize_columns_names(
        dataframe   = cns_data,
        columns_map = {'cpf': 'patient_cpf'}
    )

    merged_data = transform_merge_patient_and_cns_data(
        patient_data    = patient_data,
        cns_data        = cns_data,
    )

    data_list = transform_data_to_json(merged_data)

    data_list_batches = transform_create_input_batches(
        input = data_list
    )

    load_to_raw_endpoint.map(
        data_list_batches,
        unmapped(api_url),
        unmapped("raw/patientrecords"),
        unmapped(api_token)
    )



sms_prontuarios_raw_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_raw_smsrio.executor = LocalDaskExecutor(num_workers=10)
sms_prontuarios_raw_smsrio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi"
)

#sms_prontuarios_raw_smsrio.schedule = smsrio_daily_update_schedule
