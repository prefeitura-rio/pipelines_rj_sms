# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1123
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_conectividade_gcs.schedules import (
    schedule,
)
from pipelines.datalake.extract_load.vitacare_conectividade_gcs.tasks import (
    handle_json_files_from_gcs,
)
from pipelines.utils.time import get_datetime_working_range
from pipelines.utils.tasks import (
    create_date_partitions,
    load_files_from_gcs_bucket,
    rename_current_flow_run,
    upload_to_datalake,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Conectividade Vitacare",
) as conectividade_vitacare:
    ENVIRONMENT = Parameter("environment", default="dev")

    SOURCE_BUCKET_NAME = Parameter("source_bucket_name", default="conectividade_aps")
    SOURCE_FILE_PREFIX = Parameter("source_file_prefix", default="/")
    SOURCE_FILE_SUFFIX = Parameter("source_file_suffix", default=".json")
    DESTINATION_TABLE_NAME = Parameter("destination_table_name", default="")
    DESTINATION_DATASET_NAME = Parameter("destination_dataset_name", default="")

    START_DATETIME = Parameter("start_datetime", default="")
    END_DATETIME = Parameter("end_datetime", default="")

    rename_current_flow_run(
        name_template="Extracting {bucket_name}.{file_prefix}*{file_suffix} into {destination_dataset_name}.{destination_table_name}",  # noqa
        bucket_name=SOURCE_BUCKET_NAME,
        file_prefix=SOURCE_FILE_PREFIX,
        file_suffix=SOURCE_FILE_SUFFIX,
        destination_table_name=DESTINATION_TABLE_NAME,
        destination_dataset_name=DESTINATION_DATASET_NAME,
    )

    interval_start, interval_end = get_datetime_working_range(
        start_datetime=START_DATETIME,
        end_datetime=END_DATETIME,
        return_as_str=True,
    )

    files = load_files_from_gcs_bucket(
        bucket_name=SOURCE_BUCKET_NAME,
        file_prefix=SOURCE_FILE_PREFIX,
        file_suffix=SOURCE_FILE_SUFFIX,
        updated_after=interval_start,
        updated_before=interval_end,
        environment=ENVIRONMENT,
    )

    dataframe = handle_json_files_from_gcs(files=files)

    partition_folder = create_date_partitions(
        dataframe=dataframe, partition_column="datalake_loaded_at"
    )

    upload_to_datalake(
        input_path=partition_folder,
        dataset_id=DESTINATION_DATASET_NAME,
        table_id=DESTINATION_TABLE_NAME,
        csv_delimiter=",",
        exception_on_missing_input_file=True,
    )

conectividade_vitacare.schedule = schedule
conectividade_vitacare.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
conectividade_vitacare.executor = LocalDaskExecutor(num_workers=1)
conectividade_vitacare.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
