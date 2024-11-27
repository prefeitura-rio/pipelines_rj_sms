# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow for SMSRio Raw Data Extraction
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.raw.smsrio.constants import constants as smsrio_constants
from pipelines.prontuarios.raw.smsrio.schedules import smsrio_daily_update_schedule
from pipelines.prontuarios.raw.smsrio.tasks import (
    extract_patient_data_from_db,
    get_smsrio_database_url,
    transform_filter_invalid_cpf,
)
from pipelines.prontuarios.utils.tasks import (
    get_datetime_working_range,
    transform_split_dataframe,
)
from pipelines.utils.datalake_hub import load_asset
from pipelines.utils.tasks import load_file_from_gcs_bucket, rename_current_flow_run

####################################
# Daily Routine Flow
####################################
with Flow(
    name="Prontuários (SMSRio) - Extração de Dados",
) as sms_prontuarios_raw_smsrio:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)
    START_DATETIME = Parameter("start_datetime", default="")
    END_DATETIME = Parameter("end_datetime", default="")
    IS_INITIAL_EXTRACTION = Parameter("is_initial_extraction", default=False)

    #####################################
    # Set environment
    ####################################
    database_url = get_smsrio_database_url(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT, unidade="SMSRIO", is_initial_extraction=IS_INITIAL_EXTRACTION
        )

    ####################################
    # Task Section #1 - Get data
    ####################################
    with case(IS_INITIAL_EXTRACTION, True):
        patient_data_gcs = load_file_from_gcs_bucket(
            bucket_name=smsrio_constants.SMSRIO_BUCKET.value,
            file_name=smsrio_constants.SMSRIO_FILE_NAME.value,
        )

    with case(IS_INITIAL_EXTRACTION, False):
        start_datetime, end_datetime = get_datetime_working_range(
            start_datetime=START_DATETIME, end_datetime=END_DATETIME
        )

        patient_data_db = extract_patient_data_from_db(
            db_url=database_url, time_window_start=start_datetime, time_window_end=end_datetime
        )

    patient_data = merge(patient_data_gcs, patient_data_db)

    ####################################
    # Task Section #2 - Prepare data to load
    ####################################
    patient_valid_data = transform_filter_invalid_cpf(
        dataframe=patient_data, cpf_column="patient_cpf"
    )

    patient_data_batches = transform_split_dataframe(dataframe=patient_valid_data, batch_size=500)

    ####################################
    # Task Section #3 - Loading data
    ####################################
    load_asset.map(
        dataframe=patient_data_batches,
        asset_id=unmapped("assets.rj-sms.smsrio.paciente"),
        environment=unmapped(ENVIRONMENT),
    )


sms_prontuarios_raw_smsrio.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_prontuarios_raw_smsrio.executor = LocalDaskExecutor(num_workers=1)
sms_prontuarios_raw_smsrio.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="13.93Gi",
    memory_limit="13.93Gi",
)

sms_prontuarios_raw_smsrio.schedule = smsrio_daily_update_schedule
