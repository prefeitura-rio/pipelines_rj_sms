# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.constants import constants as prontuarios_constants
from pipelines.prontuarios.load_datalake.constants import (
    constants as datalake_constants,
)
from pipelines.prontuarios.load_datalake.schedules import (
    datalake_to_hci_daily_update_schedule,
)
from pipelines.prontuarios.load_datalake.tasks import clean_null_values, return_endpoint
from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    load_to_api,
    rename_current_flow_run,
    transform_create_input_batches,
)
from pipelines.utils.tasks import get_secret_key, load_file_from_bigquery

with Flow(
    name="Prontuários - Extraindo dados do Datalake para o HCI",
) as datalake_to_hci:
    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    TABLE_ID = Parameter("table_id", required=True)
    DATASET_ID = Parameter("dataset_id", required=True)
    PROJECT_ID = Parameter("project_id", required=True)

    ####################################
    # Set environment
    ####################################

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT, tabela=TABLE_ID, dataset=DATASET_ID
        )

    dataframe = load_file_from_bigquery(
        project_name=PROJECT_ID,
        dataset_name=DATASET_ID,
        table_name=TABLE_ID,
    )

    endpoint = return_endpoint(table_id=TABLE_ID)

    payload_clean = clean_null_values(df=dataframe, endpoint=endpoint)

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=datalake_constants.INFISICAL_PATH.value,
        infisical_api_url=prontuarios_constants.INFISICAL_API_URL.value,
        infisical_api_username=datalake_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=datalake_constants.INFISICAL_API_PASSWORD.value,
    )

    api_url = get_secret_key(
        secret_path="/",
        secret_name=prontuarios_constants.INFISICAL_API_URL.value,
        environment=ENVIRONMENT,
    )

    list_batches = transform_create_input_batches(input_list=payload_clean, batch_size=5000)

    load_to_api_task = load_to_api.map(
        request_body=list_batches,
        endpoint_name=unmapped(endpoint),
        api_token=unmapped(api_token),
        method=unmapped("PUT"),
        environment=unmapped(ENVIRONMENT),
    )


datalake_to_hci.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
datalake_to_hci.executor = LocalDaskExecutor(num_workers=6)
datalake_to_hci.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

datalake_to_hci.schedule = datalake_to_hci_daily_update_schedule
