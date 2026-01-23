# -*- coding: utf-8 -*-

import json

import pandas as pd
import requests
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.siclom_api.constants import (
    constants as siclom_constants,
)
from pipelines.datalake.extract_load.siclom_api.tasks import (
    fetch_siclom_data,
    get_patient_data,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(
    name="DataLake - Extração e Carga de Dados - SICLOM API",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.HERIAN_ID.value,
    ],
) as siclom_extraction:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    DATASET_ID = Parameter("dataset_id", default="brutos_siclom_api", required=True)
    TABLE_ID = Parameter("table_id", default="cd4", required=True)
    BATCH = Parameter("batch", default=1000, required=True)
    ENDPOINT = Parameter("endpoint", required=True)

    API_KEY = get_secret_key(
        secret_path=siclom_constants.INFISICAL_PATH.value,
        secret_name=siclom_constants.INFISICAL_NAME.value,
        environment=ENVIRONMENT,
    )

    cpf_batches = get_patient_data(environment=ENVIRONMENT, batch=BATCH)

    patient_informations = fetch_siclom_data.map(
        environment=unmapped(ENVIRONMENT),
        cpf_batch=cpf_batches,
        endpoint=unmapped(ENDPOINT),
        api_key=unmapped(API_KEY),
    )

    uploaded_data = upload_df_to_datalake.map(
        df=patient_informations,
        dataset_id=unmapped(DATASET_ID),
        table_id=unmapped(TABLE_ID),
        if_exists=unmapped("append"),
        partition_column=unmapped("extracted_at"),
        csv_delimiter=unmapped(","),
    )
