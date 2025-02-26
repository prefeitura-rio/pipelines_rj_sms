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
from pipelines.datalake.extract_load.centralregulacao_mysql.constants import SCHEMAS
from pipelines.datalake.extract_load.centralregulacao_mysql.schedules import schedule
from pipelines.datalake.extract_load.centralregulacao_mysql.tasks import (
    close_mysql,
    connect_mysql,
    get_col_names,
    query_mysql,
)
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(name="SUBGERAL - Extract & Load - Central de Regulação MySQL") as sms_cr_mysql:
    # PARAMETROS #
    # AMBIENTE ------------------------------
    ENVIRONMENT = Parameter("environment", default="dev")

    # MySQL ---------------------------------
    HOST = Parameter("host", default="db.smsrio.org", required=True)
    DATABASE = Parameter("database", default="monitoramento", required=True)
    PORT = Parameter("port", default=None, required=False)
    TABLE = Parameter("table", default="vw_MS_CadastrosAtivacoesGov")
    QUERY = Parameter("query", default="SELECT * FROM vw_MS_CadastrosAtivacoesGov")

    # BQ ------------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_centralderegulacao_mysql", required=True)

    # CREDENTIALS ---------------------------
    user = get_secret_key(environment=ENVIRONMENT, secret_name="USER", secret_path="/cr_mysql")
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="PASSWORD", secret_path="/cr_mysql"
    )

    # -----------------------------------------

    # TAREFAS #
    # 1 - conectar ao My SQL
    connection = connect_mysql(
        host=HOST, database=DATABASE, user=user, password=password, port=PORT
    )

    # 2 - obter dados
    df = query_mysql(connection=connection, query=QUERY)

    # 3 - nomear colunas
    df_column_names = get_col_names(connection=connection, df=df, table=TABLE)

    # 4 - encerrar conexão com MySQL
    close_connection = close_mysql(connection=connection, upstream_tasks=[df_column_names])

    # 5 - carregar no BQ
    upload = upload_df_to_datalake(
        df=df_column_names,
        table_id=TABLE,
        dataset_id=BQ_DATASET,
        partition_column="data_extracao",
        source_format="parquet",
    )

# CONFIGURACOES #
sms_cr_mysql.executor = LocalDaskExecutor(num_workers=3)
sms_cr_mysql.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_cr_mysql.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="10Gi",
    memory_limit="10Gi",
)
sms_cr_mysql.schedule = schedule
