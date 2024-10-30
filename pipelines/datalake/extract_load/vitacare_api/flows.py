# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
"""
Vitacare healthrecord dumping flows
"""
# from datetime import timedelta
from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_api.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_api.schedules import (
    vitacare_daily_update_schedule,
)
from pipelines.datalake.extract_load.vitacare_api.tasks import (
    create_parameter_list,
    create_partitions,
    extract_data_from_api,
    get_flow_name,
    save_data_to_file,
    transform_data,
    write_retry_results_on_bq,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.prontuarios.utils.tasks import (
    get_ap_from_cnes,
    get_current_flow_labels,
    get_healthcenter_name_from_cnes,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.tasks import (
    create_folders,
    get_project_name_from_prefect_environment,
    upload_to_datalake,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - VitaCare",
) as sms_dump_vitacare_estoque:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)
    IS_ROUTINE = Parameter("is_routine", default=True)

    # Vitacare API
    CNES = Parameter("cnes", default=None, required=True)
    TARGET_DATE = Parameter("target_date", default="")
    ENDPOINT = Parameter("endpoint", required=True)

    # GCP
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    local_folders = create_folders()

    ap = get_ap_from_cnes(cnes=CNES, upstream_tasks=[local_folders])

    with case(RENAME_FLOW, True):
        healthcenter_name = get_healthcenter_name_from_cnes(cnes=CNES)

        rename_current_flow_run(
            environment=ENVIRONMENT,
            is_routine=IS_ROUTINE,
            target_date=TARGET_DATE,
            unidade=healthcenter_name,
            endpoint=ENDPOINT,
        )

    ####################################
    # Tasks section #1 - Extract data
    ####################################
    api_data = extract_data_from_api(
        cnes=CNES,
        ap=ap,
        target_day=TARGET_DATE,
        endpoint=ENDPOINT,
        environment=ENVIRONMENT,
    )

    with case(api_data["has_data"], True):
        raw_file = save_data_to_file(
            data=api_data["data"],
            file_folder=local_folders["raw"],
            table_id=TABLE_ID,
            ap=ap,
            cnes=CNES,
            add_load_date_to_filename=True,
            load_date=TARGET_DATE,
        )

        #####################################
        # Tasks section #2 - Transform data
        #####################################

        transformed_file = transform_data(file_path=raw_file, table_id=TABLE_ID)

        #####################################
        # Tasks section #3 - Load data
        #####################################

        create_partitions_task = create_partitions(
            data_path=local_folders["raw"],
            partition_directory=local_folders["partition_directory"],
        )
        create_partitions_task.set_upstream(transformed_file)

        upload_to_datalake_task = upload_to_datalake(
            input_path=local_folders["partition_directory"],
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            if_exists="replace",
            csv_delimiter=";",
            if_storage_data_exists="replace",
            biglake_table=True,
            dataset_is_public=False,
        )
        upload_to_datalake_task.set_upstream(create_partitions_task)

    #####################################
    # Tasks section #3 - Save run metadata
    #####################################

    with case(IS_ROUTINE, False):
        write_retry_result = write_retry_results_on_bq(
            endpoint=ENDPOINT,
            response=api_data,
            ap=ap,
            cnes=CNES,
            target_date=TARGET_DATE,
            max_retries=2,
        )
        write_retry_result.set_upstream(api_data)


sms_dump_vitacare_estoque.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_estoque.executor = LocalDaskExecutor(num_workers=1)
sms_dump_vitacare_estoque.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)


# ==============================
# SCHEDULER FLOW
# ==============================

with Flow(
    name="DataLake - Agendador de Flows - VitaCare",
) as sms_dump_vitacare_estoque_scheduler:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    IS_ROUTINE = Parameter("is_routine", default=True)

    # Vitacare API
    ENDPOINT = Parameter("endpoint", required=True)
    TARGET_DATE = Parameter("target_date", default="today")
    AP = Parameter("ap", default=None)

    # GCP
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", default=None)

    #####################################
    # Set environment
    ####################################

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            endpoint=ENDPOINT,
            is_routine=IS_ROUTINE,
            target_date=TARGET_DATE,
            ap=AP,
        )

    parameter_list = create_parameter_list(
        environment=ENVIRONMENT,
        endpoint=ENDPOINT,
        is_routine=IS_ROUTINE,
        area_programatica=AP,
        target_date=TARGET_DATE,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
    )

    project_name = get_project_name_from_prefect_environment()

    current_flow_run_labels = get_current_flow_labels()

    flow_name = get_flow_name(endpoint=ENDPOINT)

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped(flow_name),
        project_name=unmapped(project_name),
        parameters=parameter_list,
        labels=unmapped(current_flow_run_labels),
    )

    wait_runs_task = wait_for_flow_run.map(
        flow_run_id=created_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
        max_duration=unmapped(timedelta(minutes=10)),
    )

sms_dump_vitacare_estoque_scheduler.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_estoque_scheduler.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare_estoque_scheduler.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
)
sms_dump_vitacare_estoque_scheduler.schedule = vitacare_daily_update_schedule
