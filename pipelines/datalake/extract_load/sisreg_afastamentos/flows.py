# -*- coding: utf-8 -*-
"""
Flow
"""
# Prefect
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GCS

from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.extract_load.sisreg_afastamentos import (
    constants,
    schedules,
)
from pipelines.datalake.extract_load.sisreg_afastamentos.tasks import (
    concat_dfs,
    get_cpf_profissionais,
    get_extraction_date,
    init_session_request_base,
    log_df,
    login_sisreg,
    search_afastamentos,
    search_historico_afastamentos,
)
from pipelines.datalake.utils.tasks import handle_columns_to_bq

# Internos
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(
    name="SUBGERAL - Extract & Load - SISREG AFASTAMENTOS",
    state_handlers=[handle_flow_state_change],
    owners=[
        pipeline_constants.MATHEUS_ID.value,
    ],
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
        "dataset_id", default=constants.DEFAULT_DATASET_ID, required=False)
    AFASTAMENTO_TABLE_ID = Parameter(
        "afastamento_table_id", default=constants.DEFAULT_AFASTAMENTO_TABLE_ID, required=False
    )
    HISTORICO_TABLE_ID = Parameter(
        "historico_table_id", default=constants.DEFAULT_HISTORICO_TABLE_ID, required=False
    )
    MIN_TASK_WAIT = Parameter(
        "min_task_wait",
        default=constants.MIN_TASK_WAIT,
        required=False,
    )
    MAX_TASK_WAIT = Parameter(
        "max_task_wait",
        default=constants.MAX_TASK_WAIT,
        required=False,
    )

    # Data de extração das tabelas
    extraction_date = get_extraction_date()

    # Requisição base do SISREG
    session = init_session_request_base()

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
        min_task_wait=unmapped(MIN_TASK_WAIT),
        max_task_wait=unmapped(MAX_TASK_WAIT),
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
        min_task_wait=unmapped(MIN_TASK_WAIT),
        max_task_wait=unmapped(MAX_TASK_WAIT),
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


sisreg_afastamentos_flow.executor = LocalDaskExecutor(num_workers=16)
sisreg_afastamentos_flow.storage = GCS(
    pipeline_constants.GCS_FLOWS_BUCKET.value
)
sisreg_afastamentos_flow.run_config = VertexRun(
    image=pipeline_constants.DOCKER_VERTEX_IMAGE.value,
    labels=[
        pipeline_constants.RJ_SMS_VERTEX_AGENT_LABEL.value,
    ],
    # https://cloud.google.com/vertex-ai/docs/training/configure-compute#machine-types
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": pipeline_constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": pipeline_constants.INFISICAL_TOKEN.value,
    },
)
sisreg_afastamentos_flow.schedule = schedules.schedule
