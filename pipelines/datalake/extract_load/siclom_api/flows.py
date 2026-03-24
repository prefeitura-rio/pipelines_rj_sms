# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.siclom_api.constants import (
    constants as siclom_constants,
)
from pipelines.datalake.extract_load.siclom_api.schedules import schedule
from pipelines.datalake.extract_load.siclom_api.tasks import (
    format_month,
    generate_formated_months,
    get_siclom_period_data,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
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
    """
    Os parametros annual, month, year e extraction_range foram pensados para permitir flexibilidade nas extrações.

    Por exemplo, se quisermos extrair os dados de um mês específico, basta preencher os campos month e year,
    deixando annual como False. Se quisermos extrair os dados de um ano específico, basta preencher o campo year
    e annual como True, o campo month será ignorado.

    O campo extraction_range serve para definir o intervalo de anos a ser extraído a partir do ano de referência,
    ou seja, se year for 2024 e extraction_range for 5, serão extraídos os dados de 2024, 2023, 2022, 2021 e 2020.
    """
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    ENDPOINT = Parameter("endpoint", required=True)
    ANNUAL = Parameter("annual", default=False, required=True)
    MONTH = Parameter("month", default=None, required=False)
    YEAR = Parameter("year", default=None, required=False)
    EXTRACTION_RANGE = Parameter("extraction_range", default=5, required=False)
    DATASET_ID = Parameter("dataset_id", default="brutos_siclom_api", required=True)
    TABLE_ID = Parameter("table_id", default="", required=True)

    BASE_URL = get_secret_key(
        secret_path=siclom_constants.INFISICAL_PATH.value,
        secret_name=siclom_constants.URL.value,
        environment=ENVIRONMENT,
    )

    API_KEY = get_secret_key(
        secret_path=siclom_constants.INFISICAL_PATH.value,
        secret_name=siclom_constants.APY_KEY.value,
        environment=ENVIRONMENT,
    )

    rename_current_flow_run(table=TABLE_ID, annual=ANNUAL, environment=ENVIRONMENT)

    with case(ANNUAL, False):
        # 1 - Formata a string do mês
        formated_month = format_month(month=MONTH, year=YEAR)

        # 2 - Faz a requisição na API do SICLOM
        month_data = get_siclom_period_data(
            base_url=BASE_URL, endpoint=ENDPOINT, api_key=API_KEY, period=formated_month
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
        formated_periods = generate_formated_months(reference_year=YEAR, interval=EXTRACTION_RANGE)

        # 2 - Faz as requisições na API do SICLOM
        months_data = get_siclom_period_data.map(
            base_url=unmapped(BASE_URL),
            period=formated_periods,
            endpoint=unmapped(ENDPOINT),
            api_key=unmapped(API_KEY),
        )

        # 3 - Carrega os dados no datalake
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
siclom_period_extraction.schedule = schedule
