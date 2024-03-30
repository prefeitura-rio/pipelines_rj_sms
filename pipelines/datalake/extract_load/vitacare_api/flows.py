# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123, C0301
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
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
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
    save_data_to_file,
    transform_data,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.prontuarios.utils.tasks import (
    get_ap_from_cnes,
    get_current_flow_labels,
    get_healthcenter_name_from_cnes,
    get_project_name,
)
from pipelines.utils.tasks import create_folders, upload_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - VitaCare",
) as sms_dump_vitacare_estoque:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)
    REPROCESS_MODE = Parameter("reprocess_mode", default=False)

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
            target_date=TARGET_DATE,
            unidade=healthcenter_name,
            endpoint=ENDPOINT,
        )

    ####################################
    # Tasks section #1 - Extract data
    #####################################
    api_data = extract_data_from_api(
        cnes=CNES,
        ap=ap,
        target_day=TARGET_DATE,
        endpoint=ENDPOINT,
        environment=ENVIRONMENT,
    )

    raw_file = save_data_to_file(
        data=api_data,
        file_folder=local_folders["raw"],
        table_id=TABLE_ID,
        ap=ap,
        cnes=CNES,
        add_load_date_to_filename=True,
        load_date=TARGET_DATE,
        upstream_tasks=[api_data],
    )

    #####################################
    # Tasks section #3 - Transform data
    #####################################

    transformed_file = transform_data(
        file_path=raw_file, table_id=TABLE_ID, upstream_tasks=[raw_file]
    )

    #####################################
    # Tasks section #4 - Load data
    #####################################

    create_partitions_task = create_partitions(
        data_path=local_folders["raw"],
        partition_directory=local_folders["partition_directory"],
        upstream_tasks=[transformed_file],
    )

    upload_to_datalake_task = upload_to_datalake(
        input_path=local_folders["partition_directory"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
        upstream_tasks=[create_partitions_task],
    )

    #####################################
    # Tasks section #3 - Save run metadata
    #####################################

    # with case(REPROCESS_MODE, True):
    #    write_on_bq_on_table_task = write_on_bq_on_table(
    #        response=download_task,
    #        dataset_id=DATASET_ID,
    #        table_id=TABLE_ID,
    #        ap=AP,
    #        cnes=CNES,
    #        data=DATE,
    #        upstream_tasks=[save_data_task],
    #    )


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

    # Vitacare API
    ENDPOINT = Parameter("endpoint", required=True)
    TARGET_DATE = Parameter("target_date", default="today")

    # GCP
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            endpoint=ENDPOINT,
            target_date=TARGET_DATE,
        )

    parameter_list = create_parameter_list(
        environment=ENVIRONMENT,
        endpoint=ENDPOINT,
        target_date=TARGET_DATE,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
    )

    project_name = get_project_name(
        environment=ENVIRONMENT,
    )

    current_flow_run_labels = get_current_flow_labels()

    created_flow_runs = create_flow_run.map(
        flow_name=unmapped("DataLake - Extração e Carga de Dados - VitaCare"),
        project_name=unmapped(project_name),
        parameters=parameter_list,
        labels=unmapped(current_flow_run_labels),
    )

    wait_runs_task = wait_for_flow_run.map(
        created_flow_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
        max_duration=unmapped(timedelta(minutes=20)),
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


# with Flow(
#    name="Dump Vitacare Reprocessamento - Reprocessar dados de Farmácia Vitacare",
# ) as sms_dump_vitacare_estoque_reprocessamento:
#    #####################################
#    # Parameters
#    #####################################
#
#    # Flow
#    RENAME_FLOW = Parameter("rename_flow", default=False)
#
#    # INFISICAL
#    INFISICAL_PATH = vitacare_constants.INFISICAL_PATH.value
#
#    # Vitacare API
#    ENDPOINT = Parameter("endpoint", required=True)
#
#    # GCP
#    ENVIRONMENT = Parameter("environment", default="dev")
#    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
#    TABLE_ID = Parameter("table_id", required=True)
#
#    # Reprocess
#    PARALLEL_RUNS = Parameter("parallel_runs", default=20)
#    LIMIT_RUNS = Parameter("limit_runs", default=None)
#
#    #####################################
#    # Set environment
#    ####################################
#    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)
#
#    with case(RENAME_FLOW, True):
#        rename_flow_task = rename_current_flow(
#            table_id=TABLE_ID, upstream_tasks=[inject_gcp_credentials_task]
#        )
#
#    ####################################
#    # Tasks section #1 - Acccess reprocessing cases
#    #####################################
#
#    retrieve_cases_task = retrieve_cases_to_reprocessed_from_birgquery(
#        dataset_id=DATASET_ID,
#        table_id=TABLE_ID,
#        query_limit=LIMIT_RUNS,
#        upstream_tasks=[inject_gcp_credentials_task],
#    )
#
#    ####################################
#    # Tasks section #2 - Reprocess cases
#    #####################################
#
#    creat_multiples_flows_runs_task = creat_multiples_flows_runs(
#        run_list=retrieve_cases_task,
#        environment=ENVIRONMENT,
#        table_id=TABLE_ID,
#        endpoint=ENDPOINT,
#        parallel_runs=PARALLEL_RUNS,
#        upstream_tasks=[retrieve_cases_task],
#    )
#
#
# sms_dump_vitacare_estoque_reprocessamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# sms_dump_vitacare_estoque_reprocessamento.executor = LocalDaskExecutor(num_workers=10)
# sms_dump_vitacare_estoque_reprocessamento.run_config = KubernetesRun(
#    image=constants.DOCKER_IMAGE.value,
#    labels=[
#        constants.RJ_SMS_AGENT_LABEL.value,
#    ],
#    memory_limit="2Gi",
# )

# sms_dump_vitacare_estoque_reprocessamento.schedule = vitacare_daily_reprocess_schedule
