# -*- coding: utf-8 -*-
"""
Fluxo
"""

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# internos
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.sisreg_api.constants import CONFIG
from pipelines.datalake.extract_load.sisreg_api.schedules import schedule
from pipelines.datalake.extract_load.sisreg_api.tasks import (
    extrair_fim,
    extrair_inicio,
    full_extract_process,
    gerar_faixas_de_data,
)
from pipelines.datalake.utils.tasks import prepare_dataframe_for_upload
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(name="SUBGERAL - Extract & Load - SISREG API") as sms_sisreg_api:
    # PARAMETROS AMBIENTE ---------------------------
    ENVIRONMENT = Parameter("environment", default="dev")

    # PARAMETROS CREDENCIAIS ------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="ES_USERNAME", secret_path="/sisreg_api"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="ES_PASSWORD", secret_path="/sisreg_api"
    )

    # PARAMETROS CONSULTA ---------------------------
    ES_INDEX = Parameter("es_index", default="solicitacao-ambulatorial-rj")
    PAGE_SIZE = Parameter("page_size", default=10_000)
    SCROLL_TIMEOUT = Parameter("scroll_timeout", default="2m")
    FILTERS = Parameter("filters", default={"codigo_central_reguladora": "330455"})
    DATA_INICIAL = Parameter("data_inicial", default="2025-01-01")
    DATA_FINAL = Parameter("data_final", default="2025-01-31")

    # PARAMETROS PARA DEFINIR TAMANHO DOS LOTES ------
    DIAS_POR_FAIXA = Parameter("dias_por_faixa", default=1)

    # PARAMETROS BQ ----------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_sisreg_api")
    BQ_TABLE = Parameter("bq_table", default="solicitacoes")

    # 1) Gera faixas de data
    faixas = gerar_faixas_de_data(
        data_inicial=DATA_INICIAL, data_final=DATA_FINAL, dias_por_faixa=DIAS_POR_FAIXA
    )

    # 2) Extrai, para cada faixa, o início e o fim do intervalo utilizando tarefas auxiliares
    inicio_faixas = extrair_inicio.map(faixa=faixas)
    fim_faixas = extrair_fim.map(faixa=faixas)

    # 3) Extrai os dados para cada faixa (executa em paralelo)
    dfs = full_extract_process.map(
        host=unmapped(CONFIG["host"]),
        port=unmapped(CONFIG["port"]),
        scheme=unmapped(CONFIG["scheme"]),
        user=unmapped(user),
        password=unmapped(password),
        index_name=unmapped(ES_INDEX),
        page_size=unmapped(PAGE_SIZE),
        scroll_timeout=unmapped(SCROLL_TIMEOUT),
        filters=unmapped(FILTERS),
        data_inicial=inicio_faixas,
        data_final=fim_faixas,
    )

    # 4) Prepara os DataFrames para upload
    dfs_prontos = prepare_dataframe_for_upload.map(
        df=dfs,
        flow_name=unmapped(CONFIG["flow_name"]),
        flow_owner=unmapped(CONFIG["flow_owner"]),
    )

    # 5) Realiza o upload para o Big Query em lotes
    uploads = upload_df_to_datalake.map(
        df=dfs_prontos,
        table_id=unmapped(BQ_TABLE),
        dataset_id=unmapped(BQ_DATASET),
        partition_column=unmapped(CONFIG["partition_column"]),
        source_format=unmapped(CONFIG["source_format"]),
    )

# Configurações de execução
sms_sisreg_api.executor = LocalDaskExecutor(num_workers=3)
sms_sisreg_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_sisreg_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request=CONFIG["memory_request"],
    memory_limit=CONFIG["memory_limit"],
)
sms_sisreg_api.schedule = schedule
