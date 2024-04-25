# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Vitai healthrecord dumping flows
"""

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.prefect import (
    task_rename_current_flow_run_dataset_table,
)

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitai_api.constants import (
    constants as vitai_constants,
)
from pipelines.datalake.extract_load.vitai_api.schedules import (
    vitai_daily_update_schedule,
)
from pipelines.datalake.extract_load.vitai_api.tasks import build_date_param, build_url
from pipelines.utils.tasks import (
    add_load_date_column,
    create_folders,
    create_partitions,
    download_from_api,
    from_json_to_csv,
    get_secret_key,
    inject_gcp_credentials,
    upload_to_datalake,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Vitai",
) as sms_dump_vitai:
    #####################################
    # Parameters
    #####################################

    # Flow
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # INFISICAL
    INFISICAL_PATH = vitai_constants.INFISICAL_PATH.value
    INFISICAL_KEY = vitai_constants.INFISICAL_KEY.value

    # Vitai API
    ENDPOINT = Parameter("endpoint", required=True)
    DATE = Parameter("date", default=None)

    # GCP
    ENVIRONMENT = Parameter("environment", default="dev")
    DATASET_ID = Parameter("dataset_id", default=vitai_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", required=True)

    #####################################
    # Set environment
    ####################################
    inject_gcp_credentials_task = inject_gcp_credentials(environment=ENVIRONMENT)

    with case(RENAME_FLOW, True):
        rename_flow_task = task_rename_current_flow_run_dataset_table(
            prefix="Dump Vitai: ", dataset_id=DATASET_ID, table_id=TABLE_ID
        )
        rename_flow_task.set_upstream(inject_gcp_credentials_task)

    ####################################
    # Tasks section #1 - Get data
    #####################################
    get_secret_task = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_KEY, environment="prod"
    )
    get_secret_task.set_upstream(inject_gcp_credentials_task)

    create_folders_task = create_folders()
    create_folders_task.set_upstream(get_secret_task)  # pylint: disable=E1101

    build_date_param_task = build_date_param(date_param=DATE)
    build_date_param_task.set_upstream(create_folders_task)

    build_url_task = build_url(endpoint=ENDPOINT, date_param=build_date_param_task)
    build_url_task.set_upstream(build_date_param_task)

    download_task = download_from_api(
        url=build_url_task,
        file_folder=create_folders_task["raw"],
        file_name=TABLE_ID,
        params=None,
        credentials=get_secret_task,
        auth_method="bearer",
        add_load_date_to_filename=True,
        load_date=build_date_param_task,
    )
    download_task.set_upstream(create_folders_task)

    #####################################
    # Tasks section #2 - Transform data and Create table
    #####################################

    conversion_task = from_json_to_csv(input_path=download_task, sep=";")
    conversion_task.set_upstream(download_task)

    add_load_date_column_task = add_load_date_column(input_path=conversion_task, sep=";")
    add_load_date_column_task.set_upstream(conversion_task)

    create_partitions_task = create_partitions(
        data_path=create_folders_task["raw"],
        partition_directory=create_folders_task["partition_directory"],
    )
    create_partitions_task.set_upstream(add_load_date_column_task)
    upload_to_datalake_task = upload_to_datalake(
        input_path=create_folders_task["partition_directory"],
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        if_exists="replace",
        csv_delimiter=";",
        if_storage_data_exists="replace",
        biglake_table=True,
        dataset_is_public=False,
    )
    upload_to_datalake_task.set_upstream(create_partitions_task)


sms_dump_vitai.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitai.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitai.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)

sms_dump_vitai.schedule = vitai_daily_update_schedule
