# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_gdrive.schedules import schedule
from pipelines.datalake.extract_load.vitacare_gdrive.tasks import (
    find_all_file_names_from_pattern,
    join_csv_files,
)
from pipelines.utils.tasks import upload_df_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - Vitacare GDrive",
) as sms_dump_vitacare_reports:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW_RUN = Parameter("rename_flow_run", default=False, required=False)
    FILE_PATTERN = Parameter("file_pattern", default=False, required=False)
    DESIRED_DATASET_NAME = Parameter("desired_dataset_name", default="brutos_prontuario_vitacare")
    DESIRED_TABLE_NAME = Parameter("desired_table_name", default=None)

    file_names = find_all_file_names_from_pattern(
        file_pattern=FILE_PATTERN,
        environment=ENVIRONMENT,
    )

    dataframe = join_csv_files(
        file_names=file_names,
        environment=ENVIRONMENT,
    )

    upload_df_to_datalake(
        df=dataframe,
        partition_column="_loaded_at",
        dataset_id=DESIRED_DATASET_NAME,
        table_id=DESIRED_TABLE_NAME,
        source_format="parquet",
        dump_mode="overwrite",
        if_exists="replace",
    )


# Storage and run configs
sms_dump_vitacare_reports.schedule = schedule
sms_dump_vitacare_reports.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_reports.executor = LocalDaskExecutor(num_workers=10)
sms_dump_vitacare_reports.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)
