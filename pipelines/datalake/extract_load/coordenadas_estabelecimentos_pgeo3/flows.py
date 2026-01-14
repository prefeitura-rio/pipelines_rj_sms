# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.coordenadas_estabelecimentos_pgeo3.schedules import (
    schedule,
)
from pipelines.datalake.extract_load.coordenadas_estabelecimentos_pgeo3.tasks import (
    enrich_coordinates,
    get_coordinates_from_address,
    get_coordinates_from_cep,
    get_estabelecimentos_sem_coordenadas,
    transform_coordinates_geopandas,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import upload_df_to_datalake

with Flow(
    "SUBGERAL - Extract & Load - Coordenadas Estabs. de Saúde API IPP",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DIT_ID.value,
    ],
) as sms_estabelecimentos_coordenadas:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # BIGQUERY ------------------------------
    BQ_DATASET_ID = Parameter("bq_dataset_id", default="brutos_geo_pgeo3", required=True)
    BQ_TABLE_ID = Parameter("bq_table_id", default="estabelecimentos_coordenadas", required=True)

    # Task 1 - Buscar estabelecimentos sem coordenadas
    df_no_coords = get_estabelecimentos_sem_coordenadas(env=ENVIRONMENT)

    # Task 2 - Gerar DataFrame com coords via CEP
    df_with_cep = get_coordinates_from_cep(df=df_no_coords)

    # Task 3 - Gerar DataFrame com coords via endereço
    df_with_address = get_coordinates_from_address(df=df_no_coords)

    # Task 4 - Combinar resultados (CEP tem precedência)
    df_enriched = enrich_coordinates(df_cep=df_with_cep, df_addr=df_with_address)

    # Task 5 - Converter formato
    df_transformed = transform_coordinates_geopandas(df=df_enriched)

    # Task 6 - Subir os dados para o Data Lake
    final = upload_df_to_datalake(
        df=df_transformed,
        table_id=BQ_TABLE_ID,
        dataset_id=BQ_DATASET_ID,
        partition_column="data_extracao",
        source_format="parquet",
    )

sms_estabelecimentos_coordenadas.executor = LocalDaskExecutor(num_workers=1)
sms_estabelecimentos_coordenadas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_estabelecimentos_coordenadas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
sms_estabelecimentos_coordenadas.schedule = schedule
