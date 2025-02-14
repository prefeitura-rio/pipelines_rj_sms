# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.enderecos_pgeo3.tasks import (
    get_estabelecimentos_sem_coordenadas,
    get_coordinates_from_cep,
    get_coordinates_from_address,
    enrich_coordinates,
    transform_coordinates_api,
)
from pipelines.datalake.extract_load.enderecos_pgeo3.schedules import schedule


with Flow(
    "Extract Load: Coordenadas (lat,long) dos estabelecimentos de saúde"
) as sms_estabelecimentos_coordenadas:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # Task 1 - Buscar estabelecimentos sem coordenadas
    df_no_coords = get_estabelecimentos_sem_coordenadas(env=ENVIRONMENT)

    # Task 2 - Gerar DataFrame com coords via CEP
    df_with_cep = get_coordinates_from_cep(df=df_no_coords)

    # Task 3 - Gerar DataFrame com coords via endereço
    df_with_address = get_coordinates_from_address(df=df_no_coords)

    # Task 4 - Combinar resultados (CEP tem precedência)
    df_enriched = enrich_coordinates(df_cep=df_with_cep, df_addr=df_with_address)

    # Task 5 - Converter formato
    df_transformed = transform_coordinates_api(df=df_enriched)

sms_estabelecimentos_coordenadas.executor = LocalDaskExecutor(num_workers=1)
sms_estabelecimentos_coordenadas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_estabelecimentos_coordenadas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
sms_estabelecimentos_coordenadas.schedule = schedule
