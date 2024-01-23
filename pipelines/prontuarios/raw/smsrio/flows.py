# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Raw Data Extraction and Load
"""
from prefect import unmapped
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.executors import LocalDaskExecutor

from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants

from pipelines.prontuarios.raw.smsrio.tasks import (
    get_database_url,
    transform_merge_patient_and_cns_data,
    transform_standardize_columns_names,
    transform_data_to_json,
)
from pipelines.prontuarios.utils.tasks import (
    get_scheduled_window,
    get_api_token,
    extract_tabledata_from_db,
    transform_filter_invalid_cpf,
    transform_create_input_batches,
    transform_to_raw_format,
    load_to_api
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
    ENVIRONMENT=Parameter("environment", default="dev")
    CNES=Parameter("cnes")

    #####################################
    # Set environment
    ####################################
    database_url=get_database_url(ENVIRONMENT)
    api_token=get_api_token(ENVIRONMENT)

    ####################################
    # Task Section #1 - Get data
    ####################################
    window_start, window_end=get_scheduled_window()

    patient_data=extract_tabledata_from_db(
        db_url=database_url,
        tablename="tb_pacientes",
        min_date=window_start,
        max_date=window_end,
        date_lookup_field="timestamp",
    )

    cns_data=extract_tabledata_from_db(
        db_url=database_url,
        tablename="tb_cns_provisorios",
        min_date=window_start,
        max_date=window_end,
        date_lookup_field="timestamp",
    )

    ####################################
    # Task Section #2 - Transform and merge data
    ####################################
    patient_data=transform_standardize_columns_names(
        dataframe=patient_data,
        columns_map={'cpf': 'patient_cpf'}
    )

    patient_data=transform_filter_invalid_cpf(
        dataframe=patient_data,
        cpf_column='patient_cpf'
    )

    merged_data=transform_merge_patient_and_cns_data(
        patient_data=patient_data,
        cns_data=cns_data,
    )

    ####################################
    # Task Section #3 - Prepare data to load
    ####################################
    json_list=transform_data_to_json(merged_data)

    json_list_batches=transform_create_input_batches(json_list)

    request_bodies=transform_to_raw_format.map(
        json_list_batches,
        unmapped(CNES),
    )

    load_to_api.map(
        request_body=request_bodies,
        endpoint_name=unmapped("raw/patientrecords"),
        api_token=unmapped(api_token),
        environment=unmapped(ENVIRONMENT),
    )


sms_prontuarios_raw_smsrio.storage=GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_raw_smsrio.executor=LocalDaskExecutor(num_workers=10)
sms_prontuarios_raw_smsrio.run_config=KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi"
)

sms_prontuarios_raw_smsrio.schedule=smsrio_daily_update_schedule
