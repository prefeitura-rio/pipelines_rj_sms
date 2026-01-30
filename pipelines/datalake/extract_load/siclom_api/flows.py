# -*- coding: utf-8 -*-
from prefect import Parameter, unmapped, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datalake.extract_load.siclom_api.constants import (
    constants as siclom_constants,
)
from pipelines.datalake.extract_load.siclom_api.tasks import (
    format_month,
    generate_formated_months,
    get_siclom_period_data
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
) as siclom_period_extraction:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    ENDPOINT = Parameter("endpoint", required=True)
    YEAR = Parameter("year", default="2025", required=True)
    ANNUAL = Parameter("annual", default=True, required=True)
    MONTH = Parameter("month", required=False)
    DATASET_ID = Parameter("dataset_id", default="brutos_siclom_api", required=True)
    TABLE_ID = Parameter("table_id", default="", required=True)
    
    API_KEY = get_secret_key(
        secret_path=siclom_constants.INFISICAL_PATH.value,
        secret_name=siclom_constants.INFISICAL_NAME.value,
        environment=ENVIRONMENT,
    )
    
    with case(ANNUAL, False):
        # 1 - Formata a string do mês
        formated_month = format_month(month=MONTH)
        
        # 2 - Faz a requisição na API do SICLOM
        month_data = get_siclom_period_data(
            endpoint=ENDPOINT,
            api_key=API_KEY,
            month=formated_month,
            year=YEAR
        )
        
        # 3 - Carrega os dados no datalake
        uploaded_data = upload_df_to_datalake(
            df=month_data,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            if_exists="append",
            partition_column="extracted_at",
            csv_delimiter=",",
        )
        
    with case(ANNUAL, True):
        # 1 - Gera as strings numéricas de cada mês formatadas
        months = generate_formated_months(_=ENVIRONMENT)
        
        # 2 - Faz as requisições na API do SICLOM
        months_data = get_siclom_period_data.map(
            year=unmapped(YEAR),
            month=months,
            endpoint=unmapped(ENDPOINT),
            api_key=unmapped(API_KEY)
        )

        #3 - Carrega os dados no datalake
        uploaded_data = upload_df_to_datalake.map(
            df=months_data,
            dataset_id=unmapped(DATASET_ID),
            table_id=unmapped(TABLE_ID),
            if_exists=unmapped("append"),
            partition_column=unmapped("extracted_at"),
            csv_delimiter=unmapped(","),
        )

siclom_period_extraction.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
siclom_period_extraction.executor = LocalDaskExecutor(num_workers=2)
siclom_period_extraction.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="4Gi",
    memory_request="2Gi",
)
