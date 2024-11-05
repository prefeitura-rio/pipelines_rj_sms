# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.tasks_clickup.constants import (
    constants as clickup_constants,
)
from pipelines.datalake.extract_load.tasks_clickup.schedules import schedule
from pipelines.datalake.extract_load.tasks_clickup.tasks import (
    extract_clickup_list_tasks,
)
from pipelines.utils.tasks import (
    get_bigquery_project_from_environment,
    get_secret_key,
    rename_current_flow_run,
    upload_to_datalake,
    create_date_partitions,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Extração de Atividades Clickup",
) as tasks_clickup_extraction:
    ENVIRONMENT = Parameter("environment", default="dev")
    LIST_ID = Parameter("list_id", default=None, required=True)
    DESTINATION_TABLE_NAME = Parameter("destination_table_name", default="")
    DESTINATION_DATASET_NAME = Parameter("destination_dataset_name", default="")

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    clickup_personal_token = get_secret_key(
        secret_name=clickup_constants.INFISICAL_KEY_CLICKUP_PERSONAL_TOKEN.value,
        secret_path=clickup_constants.INFISICAL_KEY_CLICKUP_PERSONAL_TOKEN_PATH.value,
        environment=ENVIRONMENT,
    )

    rename_current_flow_run(
        name_template="Extracting list_id={list_id} into {bigquery_project}.{destination_dataset_name}.{destination_table_name}",  # noqa
        list_id=LIST_ID,
        bigquery_project=bigquery_project,
        destination_table_name=DESTINATION_TABLE_NAME,
        destination_dataset_name=DESTINATION_DATASET_NAME,
    )

    tasks_data = extract_clickup_list_tasks(
        clickup_personal_token=clickup_personal_token,
        list_id=LIST_ID,
    )

    partition_path = create_date_partitions(
        dataframe=tasks_data,
        partition_column="datalake_loaded_at",
    )

    upload_to_datalake(
        input_path=partition_path,
        dataset_id=DESTINATION_DATASET_NAME,
        table_id=DESTINATION_TABLE_NAME,
        csv_delimiter=",",
        exception_on_missing_input_file=True,
    )

tasks_clickup_extraction.schedule = schedule
tasks_clickup_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
tasks_clickup_extraction.executor = LocalDaskExecutor(num_workers=1)
tasks_clickup_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
