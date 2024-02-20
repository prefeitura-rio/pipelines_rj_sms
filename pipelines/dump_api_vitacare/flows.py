# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitacare healthrecord dumping flows
"""
# from datetime import timedelta


from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.dump_api_vitacare.constants import constants as vitacare_constants
from pipelines.dump_api_vitacare.schedules import (
    vitacare_daily_reprocess_schedule,
    vitacare_daily_update_schedule,
)
from pipelines.dump_api_vitacare.tasks import (
    build_params,
    build_url,
    creat_multiples_flows_runs,
    create_filename,
    create_partitions,
    rename_current_flow,
    retrieve_cases_to_reprocessed_from_birgquery,
    save_data_to_file,
    write_on_bq_on_table,
)
from pipelines.utils.tasks import (
    cloud_function_request,
    create_folders,
    get_infisical_user_password,
    inject_gcp_credentials,
    upload_to_datalake,
)

with Flow(
    name="Dump Vitacare - Ingerir dados do prontuário Vitacare",
) as sms_dump_vitacare:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # INFISICAL
    INFISICAL_PATH = vitacare_constants.INFISICAL_PATH.value

    # Vitacare API
    AP = Parameter("ap", required=True, default="10")
    CNES = Parameter("cnes", default=None)
    DATE = Parameter("date", default="today")
    ENDPOINT = Parameter("endpoint", required=True)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    # Dump
    REPROCESS_MODE = Parameter("reprocess_mode", default=False)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow(
            table_id=TABLE_ID,
            ap=AP,
            cnes=CNES,
            date_param=DATE,
            upstream_tasks=[inject_gcp_credentials_task],
        )

    ####################################
    # Tasks section #1 - Get data
    #####################################
    get_infisical_user_password_task = get_infisical_user_password(
        path=INFISICAL_PATH, environment="prod", upstream_tasks=[inject_gcp_credentials_task]
    )

    create_folders_task = create_folders(upstream_tasks=[get_infisical_user_password_task])

    build_url_task = build_url(ap=AP, endpoint=ENDPOINT, upstream_tasks=[create_folders_task])

    build_params_task = build_params(date_param=DATE, cnes=CNES, upstream_tasks=[build_url_task])

    file_name_task = create_filename(table_id=TABLE_ID, ap=AP, upstream_tasks=[build_params_task])

    download_task = cloud_function_request(
        url=build_url_task,
        credential=get_infisical_user_password_task,
        request_type="GET",
        body_params=None,
        query_params=build_params_task,
        env=ENVIRONMENT,
        upstream_tasks=[file_name_task],
    )

    save_data_task = save_data_to_file(
        data=download_task["body"],
        file_folder=create_folders_task["raw"],
        table_id=TABLE_ID,
        ap=AP,
        cnes=CNES,
        add_load_date_to_filename=True,
        load_date=build_params_task["date"],
        upstream_tasks=[download_task],
    )

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    with case(save_data_task, True):

        create_partitions_task = create_partitions(
            data_path=create_folders_task["raw"],
            partition_directory=create_folders_task["partition_directory"],
            upstream_tasks=[save_data_task],
        )

        upload_to_datalake_task = upload_to_datalake(
            input_path=create_folders_task["partition_directory"],
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

    with case(REPROCESS_MODE, True):
        write_on_bq_on_table_task = write_on_bq_on_table(
            response=download_task,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            ap=AP,
            cnes=CNES,
            data=DATE,
            upstream_tasks=[save_data_task],
        )


sms_dump_vitacare.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

sms_dump_vitacare.schedule = vitacare_daily_update_schedule


with Flow(
    name="Dump Vitacare Reprocessamento - Reprocessar dados do prontuário Vitacare",
) as sms_dump_vitacare_reprocessamento:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # INFISICAL
    INFISICAL_PATH = vitacare_constants.INFISICAL_PATH.value

    # Vitacare API
    ENDPOINT = Parameter("endpoint", required=True)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    # Reprocess
    PARALLEL_RUNS = Parameter("parallel_runs", default=20)
    LIMIT_RUNS = Parameter("limit_runs", default=None)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow(
            table_id=TABLE_ID, upstream_tasks=[inject_gcp_credentials_task]
        )

    ####################################
    # Tasks section #1 - Acccess reprocessing cases
    #####################################

    retrieve_cases_task = retrieve_cases_to_reprocessed_from_birgquery(
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        query_limit=LIMIT_RUNS,
        upstream_tasks=[inject_gcp_credentials_task],
    )

    ####################################
    # Tasks section #2 - Reprocess cases
    #####################################

    creat_multiples_flows_runs_task = creat_multiples_flows_runs(
        run_list=retrieve_cases_task,
        environment=ENVIRONMENT,
        table_id=TABLE_ID,
        endpoint=ENDPOINT,
        parallel_runs=PARALLEL_RUNS,
        upstream_tasks=[retrieve_cases_task],
    )


sms_dump_vitacare_reprocessamento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_reprocessamento.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare_reprocessamento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

sms_dump_vitacare_reprocessamento.schedule = vitacare_daily_reprocess_schedule
