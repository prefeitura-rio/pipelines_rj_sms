# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitai_api.schedules import schedules
from pipelines.datalake.extract_load.vitai_api.tasks import (
    extract_data,
    format_to_upload,
    get_all_api_data,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_secret_key,
    rename_current_flow_run,
    upload_df_to_datalake,
)
from pipelines.utils.time import from_relative_date

with Flow(
    name="DataLake - Extração e Carga de Dados - Vitai API",
    state_handlers=[handle_flow_state_change],
    owners=[constants.HERIAN_ID.value],
) as sms_vitai_api:
    ENVIRONMENT = Parameter("environment", default="dev")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    # Vitai API
    RELATIVE_TARGET_DATE = Parameter("relative_target_date", default="D-1")
    ENDPOINT_PATH = Parameter("endpoint_path", default="/v1/boletim/listByPeriodo")

    # GCP
    DATASET_ID = Parameter("dataset_id", default="brutos_prontuario_vitai_api")
    TABLE_ID = Parameter("table_id", required=True)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            relative_target_date=RELATIVE_TARGET_DATE,
            endpoint_path=ENDPOINT_PATH,
        )

    target_date = from_relative_date(relative_date=RELATIVE_TARGET_DATE)

    api_token = get_secret_key(
        environment=ENVIRONMENT,
        secret_name="TOKEN",
        secret_path="/prontuario-vitai",
    )

    api_data = get_all_api_data(
        environment=ENVIRONMENT,
    )

    dataframe_per_cnes = extract_data.map(
        api_data=api_data,
        api_token=unmapped(api_token),
        endpoint_path=unmapped(ENDPOINT_PATH),
        target_date=unmapped(target_date),
    )

    full_dataframe = format_to_upload(
        responses=dataframe_per_cnes,
        endpoint_path=ENDPOINT_PATH,
        target_date=target_date,
    )

    upload_df_to_datalake(
        df=full_dataframe,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        source_format="parquet",
        partition_column="_loaded_at",
    )

sms_vitai_api.schedule = schedules
sms_vitai_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_vitai_api.executor = LocalDaskExecutor(num_workers=5)
sms_vitai_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
    memory_request="2Gi",
)
