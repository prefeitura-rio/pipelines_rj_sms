# -*- coding: utf-8 -*-
from datetime import timedelta

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_api_v2.constants import (
    constants as flow_constants,
)
from pipelines.datalake.extract_load.vitacare_api_v2.tasks import (
    extract_data,
    generate_endpoint_params,
)
from pipelines.utils.tasks import rename_current_flow_run, upload_df_to_datalake
from pipelines.utils.time import from_relative_date

with Flow(
    name="DataLake - Extração e Carga de Dados - VitaCare API v2",
) as sms_vitacare_api_v2:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # Vitacare API
    RELATIVE_TARGET_DATE = Parameter("target_date", default="D-1")
    ENDPOINT = Parameter("endpoint", required=True)

    # GCP
    DATASET_ID = Parameter("dataset_id", default=flow_constants.DATASET_ID.value)
    TABLE_ID_PREFIX = Parameter("table_id_prefix", default=None)

    #####################################
    # Tasks
    #####################################
    target_date = from_relative_date(relative_date=RELATIVE_TARGET_DATE)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            target_date=target_date,
            endpoint=ENDPOINT,
        )

    endpoint_params, table_names = generate_endpoint_params(
        target_date=target_date,
        environment=ENVIRONMENT,
        table_id_prefix=TABLE_ID_PREFIX,
    )

    extracted_data = extract_data.map(
        endpoint_params=endpoint_params,
        endpoint_name=unmapped(ENDPOINT),
        environment=unmapped(ENVIRONMENT),
    )

    upload_df_to_datalake.map(
        df=extracted_data,
        dataset_id=unmapped(DATASET_ID),
        table_id=table_names,
    )


sms_vitacare_api_v2.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_vitacare_api_v2.executor = LocalDaskExecutor(num_workers=5)
sms_vitacare_api_v2.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
    memory_request="2Gi",
)
