# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

from .tasks import (
    check_export_status,
    extract_compressed,
    request_export,
    upload_to_bigquery,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Arquivos GDB",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.AVELLAR_ID.value,
    ],
) as extract_gdb:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    URI = Parameter("gs_uri", required=True)
    DATASET = Parameter("dataset", default="brutos_gdb_{cnes/sih/sia}", required=True)
    # No caso do CNES, aqui seria o mês referência do backup
    # Ex.: "2025-08" para o backup de agosto/2025
    # O flow consegue inferir a data referência se o nome do arquivo termina com ela
    # Ex.: CNES022024.GDB -> 2024-02
    DATA_REFERENCIA = Parameter("data_referencia", default=None)
    # Se você JÁ extraiu o .GDB para um .ZIP (e deu algum erro ou precisou parar
    # o flow antes do upload pro BigQuery) você pode re-executar o flow com os
    # exatos mesmos parâmetros, mas com `from_zip: true`, e ele pega o .ZIP com
    # mesmo nome do .GDB no URI do outro parâmetro e faz o upload pra você
    FROM_ZIP = Parameter("from_zip", default=False, required=True)
    # Por padrão, o flow pede a exportação do arquivo e dorme por 1h; isso é
    # adequado para os arquivos mais recentes. Se o seu arquivo é pequeno ou
    # grande demais, você pode alterar esse tempo de espera inicial para ser
    # mais adequado
    INITIAL_BACKOFF_SECONDS = Parameter("backoff_seconds", default=60*60, required=False)
    # Alguns arquivos possuem muitas e muitas colunas, então a quantidade padrão de
    # linhas (100k) pode estourar a memória; aqui você pode definir um valor menor
    CHUNK_SIZE = Parameter("lines_per_chunk", default=100_000, required=False)

    with case(FROM_ZIP, False):
        task_id = request_export(uri=URI, environment=ENVIRONMENT)
        result_uri = check_export_status(uuid=task_id, environment=ENVIRONMENT, backoff=INITIAL_BACKOFF_SECONDS)
        path = extract_compressed(uri=result_uri, environment=ENVIRONMENT)
        upload_to_bigquery(
            path=path,
            dataset=DATASET,
            uri=URI,
            refdate=DATA_REFERENCIA,
            environment=ENVIRONMENT,
            lines_per_chunk=CHUNK_SIZE
        )
    with case(FROM_ZIP, True):
        path = extract_compressed(uri=URI, environment=ENVIRONMENT)
        upload_to_bigquery(
            path=path,
            dataset=DATASET,
            uri=URI,
            refdate=DATA_REFERENCIA,
            environment=ENVIRONMENT,
            lines_per_chunk=CHUNK_SIZE
        )


extract_gdb.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
extract_gdb.executor = LocalDaskExecutor(num_workers=1)
extract_gdb.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
    memory_request="2Gi",
)
