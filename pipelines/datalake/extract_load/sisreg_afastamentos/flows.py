# -*- coding: utf-8 -*-
"""
Flow
"""
# Prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# Internos
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import constants as pipeline_constants

from pipelines.datalake.extract_load.sisreg_afastamentos import (
    schedules
)
from pipelines.datalake.extract_load.sisreg_afastamentos import (
    constants
)
from pipelines.datalake.extract_load.sisreg_afastamentos.tasks import (
    concat_dfs,
    get_base_request,
    get_cpf_profissionais,
    get_extraction_date,
    log_df,
    login_sisreg,
    search_afastamentos,
    search_historico_afastamentos,
)
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(
    name="SUBGERAL - Extract & Load - SISREG AFASTAMENTOS",
    state_handlers=[handle_flow_state_change],
) as sisreg_afastamentos_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # Resgatando usuário e senha do das secrets
    usuario = get_secret_key(
        secret_path=constants.INFISICAL_SISREG_PATH,
        secret_name=constants.INFISICAL_SISREG_USERNAME,
        environment=ENVIRONMENT,
    )
    senha = get_secret_key(
        secret_path=constants.INFISICAL_SISREG_PATH,
        secret_name=constants.INFISICAL_SISREG_PASSWORD,
        environment=ENVIRONMENT,
    )

    # Nome do dataset e tabela no datalake
    DATASET_ID = Parameter(
        "dataset_id",
        default=constants.DEFAULT_DATASET_ID,
        required=False
    )
    AFASTAMENTO_TABLE_ID = Parameter(
        "afastamento_table_id",
        default=constants.DEFAULT_AFASTAMENTO_TABLE_ID,
        required=False
    )
    HISTORICO_TABLE_ID = Parameter(
        "historico_table_id",
        default=constants.DEFAULT_HISTORICO_TABLE_ID,
        required=False
    )

    # Data de extração das tabelas
    extraction_date = get_extraction_date()

    # Requisição base do SISREG
    session = get_base_request()

    # Buscando os CPFs dos profissionais,
    # com limite adicionado para questẽs de teste.
    df_cpfs = get_cpf_profissionais(
        environment=ENVIRONMENT,
    )

    session_after_login = login_sisreg(
        usuario=usuario,
        senha=senha,
        session=session,
    )

    # Pagina de afastamentos
    dfs_afastamentos = search_afastamentos.map(
        cpf=df_cpfs,
        session=unmapped(session_after_login),
        extraction_date=unmapped(extraction_date),
    )

    # Junção dos dados de afastamento gerados
    # Incluindo preparação e upload dos mesmo
    df_afastamento = concat_dfs(dfs=dfs_afastamentos)
    df_afastamento_ok = handle_columns_to_bq(df=df_afastamento)
    log_df(df=df_afastamento_ok, name=AFASTAMENTO_TABLE_ID)
    upload_df_to_datalake(
        df=df_afastamento_ok,
        dataset_id=DATASET_ID,
        table_id=AFASTAMENTO_TABLE_ID,
        partition_column=constants.EXTRACTION_DATE_COLUMN,
        source_format="parquet",
    )

    # Pagina de histórico de afastamentos
    # Mais detalhada
    dfs_historicos = search_historico_afastamentos.map(
        cpf=df_cpfs,
        session=unmapped(session_after_login),
        extraction_date=unmapped(extraction_date),
    )

    # Junção dos dados de historico gerados
    # Incluindo preparação e upload dos mesmo
    df_historico = concat_dfs(dfs=dfs_historicos)
    df_historico_ok = handle_columns_to_bq(df=df_historico)
    log_df(df=df_historico_ok, name=HISTORICO_TABLE_ID)
    upload_df_to_datalake(
        df=df_historico_ok,
        dataset_id=DATASET_ID,
        table_id=HISTORICO_TABLE_ID,
        partition_column=constants.EXTRACTION_DATE_COLUMN,
        source_format="parquet",
    )


sisreg_afastamentos_flow.executor = LocalDaskExecutor(num_workers=1)
sisreg_afastamentos_flow.storage = GCS(
    pipeline_constants.GCS_FLOWS_BUCKET.value
)
sisreg_afastamentos_flow.run_config = KubernetesRun(
    image=pipeline_constants.DOCKER_IMAGE.value,
    labels=[pipeline_constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="10Gi",
    memory_limit="10Gi",
)
sisreg_afastamentos_flow.schedule = schedules.schedule
