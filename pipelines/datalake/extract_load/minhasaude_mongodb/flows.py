# -*- coding: utf-8 -*-
"""
Fluxo
"""

# Prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# Internas
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.minhasaude_mongodb.schedules import schedule

# Tarefas
from pipelines.datalake.extract_load.minhasaude_mongodb.tasks import (
    close_mongodb_connection,
    create_mongodb_connection,
    get_collection_data,
    to_pandas,
)
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow("Extract Load: Minha Saúde Rio - MongoDB") as minhasaude_mongodb_flow:
    ###### Parâmetros ############################
    # Geral ------------------------------
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # MONGO ------------------------------
    MONGO_HOST = Parameter("host", default="db.smsrio.org", required=True)
    MONGO_PORT = Parameter("port", default=27017, required=True)
    MONGO_AUTHSOURCE = Parameter("authsource", default="minhasauderio", required=True)
    MONGO_DATABASE = Parameter("database", default="minhasauderio", required=True)
    MONGO_COLLECTION = Parameter("collection", default="perfil_acessos", required=True)
    MONGO_QUERY = Parameter("query", default={}, required=False)
    MONGO_SAMPLE_SIZE = Parameter("sample_size", default=100, required=False)

    # BIGQUERY ------------------------------
    BQ_DATASET_ID = Parameter("bq_dataset_id", default="brutos_minhasaude_mongodb", required=True)
    BQ_TABLE_ID = Parameter("bq_table_id", default="perfil_acessos", required=True)

    # INFISICAL ------------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="USER", secret_path="/minhasaude_mongodb"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="PASSWORD", secret_path="/minhasaude_mongodb"
    )

    ###### Tarefas ############################
    # Tarefa 1 - Cria conexão com o MongoDB
    client, db = create_mongodb_connection(
        host=MONGO_HOST,
        port=MONGO_PORT,
        user=user,
        password=password,
        authsource=MONGO_AUTHSOURCE,
        db_name=MONGO_DATABASE,
    )

    # Tarefa 2 - Obtem dados da coleção
    data = get_collection_data(
        db=db, collection_name=MONGO_COLLECTION, query=MONGO_QUERY, sample_size=MONGO_SAMPLE_SIZE
    )

    # Tarefa 3 - Transforma os dados em um DataFrame
    df = to_pandas(data=data)

    # Tarefa 4 - Upload do DataFrame para o Data Lake
    upload_df_to_datalake(
        df=df,
        table_id=BQ_TABLE_ID,
        dataset_id=BQ_DATASET_ID,
        partition_column="data_extracao",
        source_format="parquet",
    )

    # Tarefa 5 - Fecha a conexão com o MongoDB
    close_mongodb_connection(client=client, upstream_tasks=[data])

minhasaude_mongodb_flow.executor = LocalDaskExecutor(num_workers=1)
minhasaude_mongodb_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
minhasaude_mongodb_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
minhasaude_mongodb_flow.schedule = schedule
