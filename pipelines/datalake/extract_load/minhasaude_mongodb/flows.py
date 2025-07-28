# -*- coding: utf-8 -*-
"""
Fluxo
"""

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.minhasaude_mongodb.schedules import schedule
from pipelines.datalake.extract_load.minhasaude_mongodb.tasks import (
    extrair_fatia_para_datalake,
    gerar_faixas_de_fatiamento,
    validar_total_documentos,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key

with Flow(
    "SUBGERAL - Extract & Load - MinhaSaude.rio MongoDB (paginado)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
    ],
) as minhasaude_mongodb_flow:
    # Parâmetros -----------------------------------------------------------
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # Dados do administrador do fluxo --------------------------------------------
    FLOW_NAME = Parameter("flow_name", default="MinhaSaude.rio MongoDB", required=True)
    FLOW_OWNER = Parameter("flow_owner", default="1184846547242995722", required=True)

    # MongoDB --------------------------------------------------------------
    MONGO_HOST = Parameter("host", default="db.smsrio.org", required=True)
    MONGO_PORT = Parameter("port", default=27017, required=True)
    MONGO_AUTHSOURCE = Parameter("authsource", default="minhasauderio", required=True)
    MONGO_DATABASE = Parameter("database", default="minhasauderio", required=True)
    MONGO_COLLECTION = Parameter("collection", default="perfil_acessos", required=True)
    MONGO_QUERY = Parameter("query", default={}, required=False)

    # Paginação -----------------------------------------------------------
    SLICE_VAR = Parameter("slice_var", default="createdAt", required=True)
    SLICE_SIZE = Parameter("slice_size", default=30, required=True)

    # BigQuery -------------------------------------------------------------
    BQ_DATASET_ID = Parameter("bq_dataset_id", default="brutos_minhasaude_mongodb", required=True)
    BQ_TABLE_ID = Parameter("bq_table_id", default="perfil_acessos", required=True)

    # Credenciais ----------------------------------------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="USER", secret_path="/minhasaude_mongodb"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="PASSWORD", secret_path="/minhasaude_mongodb"
    )

    # Tarefas -------------------------------------
    lista_faixas, n_documentos = gerar_faixas_de_fatiamento(
        host=MONGO_HOST,
        port=MONGO_PORT,
        user=user,
        password=password,
        authsource=MONGO_AUTHSOURCE,
        db_name=MONGO_DATABASE,
        collection_name=MONGO_COLLECTION,
        query=MONGO_QUERY,
        slice_var=SLICE_VAR,
        slice_size=SLICE_SIZE,
    )

    n_documentos_enviados = extrair_fatia_para_datalake.map(
        host=unmapped(MONGO_HOST),
        port=unmapped(MONGO_PORT),
        user=unmapped(user),
        password=unmapped(password),
        authsource=unmapped(MONGO_AUTHSOURCE),
        db_name=unmapped(MONGO_DATABASE),
        collection_name=unmapped(MONGO_COLLECTION),
        query=unmapped(MONGO_QUERY),
        slice_var=unmapped(SLICE_VAR),
        bq_dataset_id=unmapped(BQ_DATASET_ID),
        bq_table_id=unmapped(BQ_TABLE_ID),
        flow_name=unmapped(FLOW_NAME),
        flow_owner=unmapped(FLOW_OWNER),
        faixa=lista_faixas,
    )

    validacao = validar_total_documentos(
        total_esperado=n_documentos,
        docs_por_fatia=n_documentos_enviados,
        flow_name=FLOW_NAME,
        flow_owner=FLOW_OWNER,
    )

minhasaude_mongodb_flow.executor = LocalDaskExecutor(num_workers=10)
minhasaude_mongodb_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
minhasaude_mongodb_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="13Gi",
    memory_limit="13Gi",
)
minhasaude_mongodb_flow.schedule = schedule
