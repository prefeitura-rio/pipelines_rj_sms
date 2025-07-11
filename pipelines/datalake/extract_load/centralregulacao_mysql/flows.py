# -*- coding: utf-8 -*-
"""
Fluxo
"""

# prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.centralregulacao_mysql.schedules import schedule
from pipelines.datalake.extract_load.centralregulacao_mysql.tasks import (
    query_mysql_all_in_one,
)
from pipelines.datalake.utils.tasks import handle_columns_to_bq

# internos
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(
    name=" SUBGERAL - Extract & Load - Central de Regulação (MySQL) ",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
    ],
) as sms_cr_mysql:
    # PARAMETROS #
    # AMBIENTE ------------------------------
    ENVIRONMENT = Parameter("environment", default="dev")

    # MySQL ---------------------------------
    HOST = Parameter("host", default="db.smsrio.org")
    DATABASE = Parameter("database", default="monitoramento")
    PORT = Parameter("port", default=None)
    TABLE = Parameter("table", default="vw_MS_CadastrosAtivacoesGov")
    QUERY = Parameter("query", default="SELECT * FROM vw_MS_CadastrosAtivacoesGov")

    # BQ ------------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_centralderegulacao_mysql")

    # CREDENTIALS ---------------------------
    user = get_secret_key(environment=ENVIRONMENT, secret_name="USER", secret_path="/cr_mysql")
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="PASSWORD", secret_path="/cr_mysql"
    )

    # -----------------------------------------

    # TAREFAS #
    # 1 - obter os dados do MySQL
    df = query_mysql_all_in_one(
        host=HOST,
        database=DATABASE,
        user=user,
        password=password,
        port=PORT,
        table=TABLE,
        query=QUERY,
    )

    # 2 - transforma colunas para adequação ao Big Query
    df_columns_ok = handle_columns_to_bq(df=df)

    # 3 - carregar no BQ
    upload = upload_df_to_datalake(
        df=df_columns_ok,
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
