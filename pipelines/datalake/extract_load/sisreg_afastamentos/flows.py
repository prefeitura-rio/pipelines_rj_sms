# -*- coding: utf-8 -*-
"""
Flow
"""
# Prefect
from prefect import Parameter
# from prefect.executors import LocalDaskExecutor
# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS
from prefect.utilities.edges import unmapped

# Internos
from prefeitura_rio.pipelines_utils.custom import Flow

# from pipelines.constants import constants
# from pipelines.datalake.extract_load.sisreg_afastamentos.schedules import schedule
from pipelines.datalake.extract_load.sisreg_afastamentos.constants import (
    DEFAULT_DATASET_ID,
    EXTRACTION_DATE_COLUMN,
    AFASTAMENTO_TABLE_NAME,
    HISTORICO_TABLE_NAME,
)
from pipelines.datalake.extract_load.sisreg_afastamentos.tasks import (
    get_cpf_profissionais,
    get_base_request,
    get_extraction_date,
    login_sisreg,
    search_afastamentos,
    search_historico_afastamentos,
    concat_dfs,
    log_df,
)
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_secret_key,
    upload_df_to_datalake,
)


with Flow(
    name="SUBGERAL - Extract & Load - SISREG AFASTAMENTOS",
    state_handlers=[handle_flow_state_change],
) as sisreg_afastamentos_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # Usuario e senha do SISREG
    USUARIO = Parameter("usuario_sisreg", required=True)
    SENHA = Parameter("senha_sisreg", required=True)
    # usuario = get_secret_key(
    #     secret_path="/sisreg",
    #     secret_name="usuario",
    #     environment=ENVIRONMENT,
    # )
    #
    # senha = get_secret_key(
    #     secret_path="/sisreg",
    #     secret_name="senha",
    #     environment=ENVIRONMENT,
    # )

    # Nome do dataset no datalake
    DATASET_ID = Parameter(
        "dataset_id",
        default=DEFAULT_DATASET_ID,
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
        # limit=10,
    )

    session_after_login = login_sisreg(
        usuario=USUARIO,
        senha=SENHA,
        session=session,
    )
    # session_after_login = login_sisreg(
    #     usuario=usuario,
    #     senha=senha,
    #     session=session,
    # )

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
    log_df(df=df_afastamento, name=AFASTAMENTO_TABLE_NAME)
    upload_df_to_datalake(
        df=df_afastamento_ok,
        dataset_id=DATASET_ID,
        table_id=AFASTAMENTO_TABLE_NAME,
        partition_column=EXTRACTION_DATE_COLUMN,
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
    log_df(df=df_historico, name=HISTORICO_TABLE_NAME)
    upload_df_to_datalake(
        df=df_historico_ok,
        dataset_id=DATASET_ID,
        table_id=HISTORICO_TABLE_NAME,
        partition_column=EXTRACTION_DATE_COLUMN,
    )
