# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants

from pipelines.datalake.extract_load.vitai_db.tasks import (
    get_table_names,
    download_table_data_to_parquet,
)
from pipelines.prontuarios.utils.tasks import (
    get_datetime_working_range,
    rename_current_flow_run
)
from pipelines.utils.tasks import (
   upload_to_datalake,
   get_secret_key
)

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde)",
) as sms_dump_vitai_rio_saude:
    #####################################
    # Tasks section #1 - Listing Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    START_DATETIME = Parameter("start_datetime", default="")
    END_DATETIME = Parameter("end_datetime", default="")

    #####################################
    # Tasks section #2 - Setup
    #####################################
    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=START_DATETIME, end_datetime=END_DATETIME
    )

    db_url = get_secret_key(
        environment=ENVIRONMENT,
        secret_name="DB_URL",
        secret_path="/prontuario-vitai"
    )

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            start_datetime=start_datetime,
            end_datetime=end_datetime
        )

    #####################################
    # Tasks section #3 - Downloading Table Data
    #####################################
    table_names = get_table_names()

    table_data_files = download_table_data_to_parquet.map(
        db_url=unmapped(db_url),
        table_name=table_names,
        start_datetime=unmapped(start_datetime),
        end_datetime=unmapped(end_datetime),
    )

    #####################################
    # Tasks section #2 - Uploading to Datalake
    #####################################
    upload_to_datalake_task = upload_to_datalake.map(
        input_path=table_data_files,
        dataset_id=unmapped("vitai_db"),
        table_id=table_names,
        if_exists=unmapped("replace"),
        source_format=unmapped("parquet"),
        if_storage_data_exists=unmapped("append"),
        biglake_table=unmapped(True),
        dataset_is_public=unmapped(False),
    )

sms_dump_vitai_rio_saude.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitai_rio_saude.executor = LocalDaskExecutor(num_workers=1)
sms_dump_vitai_rio_saude.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)