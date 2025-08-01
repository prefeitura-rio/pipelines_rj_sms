# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.gal_gcs.schedules import schedule
from pipelines.datalake.extract_load.gal_gcs.tasks import list_all_files, process_file
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="DataLake - Extração e Carga de Dados - GAL",
    state_handlers=[handle_flow_state_change],
    owners=[constants.PEDRO_ID.value],
) as extract_gal_gcs:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    GCS_BUCKET = Parameter("gcs_bucket", default="svs_cie", required=True)
    GCS_PATH_TEMPLATE = Parameter(
        "gcs_path_template", default="exames_laboratoriais/gal_lacen/tuberculose/*/*/*.zip"
    )
    DATASET_ID = Parameter("dataset_id", default="brutos_gal")
    TABLE_ID = Parameter("table_id", default="exames_laboratoriais")
    RELATIVE_TARGET_DATE = Parameter("relative_target_date", default=None)

    files = list_all_files(
        gcs_bucket=GCS_BUCKET,
        gcs_path_template=GCS_PATH_TEMPLATE,
        relative_target_date=RELATIVE_TARGET_DATE,
        environment=ENVIRONMENT,
    )

    process_file.map(
        gcs_file_path=files,
        gcs_bucket=unmapped(GCS_BUCKET),
        dataset=unmapped(DATASET_ID),
        table=unmapped(TABLE_ID),
    )

extract_gal_gcs.schedule = schedule
extract_gal_gcs.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_gal_gcs.executor = LocalDaskExecutor(num_workers=1)
extract_gal_gcs.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
    memory_request="5Gi",
)
