# -*- coding: utf-8 -*-
"""
Flow
"""
# Prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GCS

# Internos
from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

from pipelines.datalake.extract_load.sisreg_afastamentos import (
    constants,
    schedules,
)
from pipelines.datalake.extract_load.sisreg_afastamentos.tasks import (
    get_cpf_profissionais,
    get_extraction_date,
    init_session_request_base,
    log_df,
    login_sisreg,
    get_afastamentos,
)

with Flow(
    name="SUBGERAL/CR/NTI - Extract & Load - SISREG Afastamentos",
    state_handlers=[handle_flow_state_change],
    owners=[
        pipeline_constants.MATHEUS_ID.value,
    ],
) as sisreg_afastamentos_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # Nome do dataset e tabela no datalake
    DATASET_ID = Parameter(
        "dataset_id", default=constants.DEFAULT_DATASET_ID, required=False
    )
    AFASTAMENTO_TABLE_ID = Parameter(
        "afastamento_table_id", default=constants.DEFAULT_AFASTAMENTO_TABLE_ID, required=False
    )
    HISTORICO_TABLE_ID = Parameter(
        "historico_table_id", default=constants.DEFAULT_HISTORICO_TABLE_ID, required=False
    )
    SLEEP_TIME = Parameter(
        "min_task_wait",
        default=constants.SLEEP_TIME,
        required=False,
    )

    # Resgatando usuário e senha das secrets
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

    # Data de extração das tabelas
    extraction_date = get_extraction_date()

    # Requisição base do SISREG
    session = init_session_request_base()

    # Buscando os CPFs dos profissionais,
    # com limite adicionado para questões de teste.
    cpf_list = get_cpf_profissionais(
        environment=ENVIRONMENT,
        extraction_date=extraction_date,
    )

    session_after_login = login_sisreg(
        usuario=usuario,
        senha=senha,
        session=session,
    )

    # Pagina de afastamentos
    df_afastamentos = get_afastamentos(
        session=session,
        cpf_list=cpf_list,
        extraction_date=extraction_date,
        sleep_time=SLEEP_TIME,
        historico=False,
    )

    # Tratamento das colunas dos dados de afastamento
    df_afastamentos_ok = handle_columns_to_bq(df=df_afastamentos)
    # Log do dado obtido
    log_df(df=df_afastamentos_ok, name=AFASTAMENTO_TABLE_ID)
    # Upload do dado de afastamento
    upload_df_to_datalake(
        df=df_afastamentos_ok,
        dataset_id=DATASET_ID,
        table_id=AFASTAMENTO_TABLE_ID,
        partition_column=constants.EXTRACTION_DATE_COLUMN,
        source_format="parquet",
    )

    # Pagina de historico de afastamentos
    df_historico_afastamentos = get_afastamentos(
        session=session,
        cpf_list=cpf_list,
        extraction_date=extraction_date,
        sleep_time=SLEEP_TIME,
        historico=True,
    )

    # Tratamento das colunas dos dados de historico de afastamento
    df_historico_afastamentos_ok = handle_columns_to_bq(
        df=df_historico_afastamentos
    )
    # Log do dado obtido
    log_df(df=df_historico_afastamentos_ok, name=HISTORICO_TABLE_ID)
    # Upload do dado de historico_afastamento
    upload_df_to_datalake(
        df=df_historico_afastamentos_ok,
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
