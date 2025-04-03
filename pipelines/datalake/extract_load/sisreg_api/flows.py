# -*- coding: utf-8 -*-
"""
Fluxo
"""

# prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# internos
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants

from pipelines.datalake.utils.tasks import prepare_dataframe_for_upload
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

from pipelines.datalake.extract_load.sisreg_api.schedules import schedule
from pipelines.datalake.extract_load.sisreg_api.tasks import (
    connect_elasticsearch,
    iniciar_consulta,
    processar_lote_inicial,
    continuar_scroll_e_processar,
    limpar_scroll,
)

from pipelines.datalake.extract_load.sisreg_api.constants import CONFIG

# ------------------------------------------

with Flow(name="SUBGERAL - Extract & Load - SISREG API") as sms_sisreg_api:
    # PARAMETROS AMBIENTE ---------------------------
    ENVIRONMENT = Parameter("environment", default="dev")

    # PARAMETROS CREDENCIAIS ------------------------
    user = get_secret_key(
        environment=ENVIRONMENT,
        secret_name="ES_USERNAME",
        secret_path="/sisreg_api"
    )
    password = get_secret_key(
        environment=ENVIRONMENT,
        secret_name="ES_PASSWORD",
        secret_path="/sisreg_api"
    )

    # PARAMETROS CONSULTA ----------------------------
    ES_INDEX = Parameter("es_index", default="solicitacao-ambulatorial-rj")
    PAGE_SIZE = Parameter("page_size", default=10_000)
    SCROLL_TIMEOUT = Parameter("scroll_timeout", default="2m")
    FILTERS = Parameter("filters", default={"codigo_central_reguladora": "330455"})
    DATA_INICIAL = Parameter("data_inicial", default="2025-01-01")
    DATA_FINAL = Parameter("data_final", default="now")

    # PARAMETROS BQ -----------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_sisreg_api")
    BQ_TABLE = Parameter("bq_table", default="solicitacoes")

    # ------------------------------------------

    # TAREFA 1. Conecta a API do SISREG (Elasticsearch)
    es_client = connect_elasticsearch(
        host=CONFIG["host"],
        port=CONFIG["port"],
        scheme=CONFIG["scheme"],
        user=user,
        password=password
    )

    # TAREFA 2. Inicia consulta
    scroll_id, total_registros, hits_iniciais = iniciar_consulta(
        client=es_client,
        index_name=ES_INDEX,
        page_size=PAGE_SIZE,
        scroll_timeout=SCROLL_TIMEOUT,
        filters=FILTERS,
        data_inicial=DATA_INICIAL,
        data_final=DATA_FINAL,
    )

    # TAREFA 3. Processa lote inicial
    dados_processados = processar_lote_inicial(
        hits_iniciais=hits_iniciais,
        total_registros=total_registros
    )

    # TAREFA 4. Continua consulta (com paginação)
    df, scroll_ids_usados = continuar_scroll_e_processar(
        client=es_client,
        scroll_id=scroll_id,
        scroll_timeout=SCROLL_TIMEOUT,
        total_registros=total_registros,
        ja_processados=dados_processados,
    )

    # TAREFA 5. Limpa resíduos da paginação
    limpar_scroll(client=es_client,
                  scroll_ids=scroll_ids_usados)

    # TAREFA 6. Prepara o DataFrame para upload
    df_final = prepare_dataframe_for_upload(
        df=df,
        flow_name=CONFIG["flow_name"],
        flow_owner=CONFIG["flow_owner"]
    )

    # TAREFA 7. Sobe os dados para o Big Query
    upload = upload_df_to_datalake(
        df=df,
        table_id=BQ_TABLE,
        dataset_id=BQ_DATASET,
        partition_column=CONFIG["partition_column"],
        source_format=CONFIG["source_format"],
    )

# ------------------------------------------

# CONFIGURACOES #
sms_sisreg_api.executor = LocalDaskExecutor(num_workers=3)
sms_sisreg_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_sisreg_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request=CONFIG["memory_request"],
    memory_limit=CONFIG["memory_limit"],
)
sms_sisreg_api.schedule = schedule
