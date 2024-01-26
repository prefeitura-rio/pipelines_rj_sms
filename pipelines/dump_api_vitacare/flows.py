# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitacare healthrecord dumping flows
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.executors import LocalDaskExecutor
from prefect.tasks.prefect import create_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.dump_api_vitacare.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.tasks import (
    inject_gcp_credentials,
    get_infisical_user_password,
    create_folders,
    cloud_function_request,
    create_partitions,
    upload_to_datalake,
)
from pipelines.dump_api_vitacare.tasks import (
    rename_current_flow,
    build_url,
    build_params,
    create_filename,
    save_data_to_file,
    retrieve_cases_to_reprocessed_from_birgquery,
)

from pipelines.dump_api_vitacare.schedules import vitacare_clocks


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
    ENDPOINT = Parameter("endpoint", required=True)
    DATE = Parameter("date", default="today")
    CNES = Parameter("cnes", default=None)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow(table_id=TABLE_ID, ap=AP, cnes=CNES)

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
        data=download_task,
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


sms_dump_vitacare.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

sms_dump_vitacare.schedule = vitacare_clocks


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
    AP = Parameter("ap", required=True, default="10")
    # ENDPOINT = Parameter("endpoint", required=True)
    # DATE = Parameter("date", default="today")
    CNES = Parameter("cnes", default=None)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=vitacare_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow(table_id=TABLE_ID, ap=AP, cnes=CNES)

    ####################################
    # Tasks section #1 - Access reprocessing cases
    #####################################

    retrieve_cases_task = retrieve_cases_to_reprocessed_from_birgquery(
        upstream_tasks=[inject_gcp_credentials_task]
    )

    ####################################
    # Tasks section #2 - Reprocess cases
    #####################################

    dump_to_gcs_flow = create_flow_run(
        flow_name="Dump Vitacare - Ingerir dados do prontuário Vitacare",
        project_name="staging",
        parameters={
            "ap": "10",
            "endpoint": "movimento",
            "table_id": "estoque_movimento",
            "environment": "dev",
            "date": "2024-01-19",
            "cnes": "6023975",
            "rename_flow": False,
        },
        run_name=f"Reprocessamento - {TABLE_ID} - {CNES}",
        upstream_tasks=[retrieve_cases_task],
    )
