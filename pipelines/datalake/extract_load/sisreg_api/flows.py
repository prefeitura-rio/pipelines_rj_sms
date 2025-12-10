# -*- coding: utf-8 -*-
"""
Fluxo
"""

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.sisreg_api.constants import CONFIG
from pipelines.datalake.extract_load.sisreg_api.schedules import schedule
from pipelines.datalake.extract_load.sisreg_api.tasks import (
    full_extract_process,
    gera_data_inicial,
    make_run_meta,
    validate_upload,
    mark_slice_completed
)
from pipelines.datalake.utils.tasks import (
    delete_file,
    extrair_fim,
    extrair_inicio,
    gerar_faixas_de_data,
    prepare_df_from_disk,
    upload_from_disk,
)

# internos
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow(
    name="SUBGERAL - Extract & Load - SISREG API",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
    ],
) as sms_sisreg_api:
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
    DATA_INICIAL = Parameter("data_inicial", default="")
    DATA_FINAL = Parameter("data_final", default="2025-01-31")

    # PARAMETROS PARA DEFINIR TAMANHO DOS LOTES ------
    DIAS_POR_FAIXA = Parameter("dias_por_faixa", default=1)
    FORMATO_DATA = Parameter("formato_data", default="%Y-%m-%d")

    # PARAMETROS BQ ----------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_sisreg_api")
    BQ_TABLE = Parameter("bq_table", default="solicitacoes")

    data_inicial = gera_data_inicial(data_inicio=DATA_INICIAL, data_fim=DATA_FINAL)

    faixas = gerar_faixas_de_data(
        data_inicial=data_inicial,
        data_final=DATA_FINAL,
        dias_por_faixa=DIAS_POR_FAIXA,
        date_format=FORMATO_DATA,
    )

    inicio_faixas = extrair_inicio.map(faixa=faixas)
    fim_faixas = extrair_fim.map(faixa=faixas)

    # 0) Metadados de execução
    run_id, as_of = make_run_meta()

    # 1) Extrai e salva cada lote em disco, retorna caminho do parquet
    raw_files = full_extract_process.map(
        run_id=unmapped(run_id),
        as_of=unmapped(as_of),
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

    # 2) Prepara cada arquivo (lê e gera outro parquet)
    prepared_files = prepare_df_from_disk.map(
        file_path=raw_files,
        flow_name=unmapped(CONFIG["flow_name"]),
        flow_owner=unmapped(CONFIG["flow_owner"]),
    )

    # 3) Faz o upload lendo cada arquivo preparado do disco
    uploads = upload_from_disk.map(
        file_path=prepared_files,
        table_id=unmapped(BQ_TABLE),
        dataset_id=unmapped(BQ_DATASET),
        partition_column=unmapped(CONFIG["partition_column"]),
        source_format=unmapped(CONFIG["source_format"]),
    )

    # 4) Exclui os arquivos após o upload
    delete_raw = delete_file.map(file_path=raw_files, upstream_tasks=[uploads])
    delete_prepared = delete_file.map(file_path=prepared_files, upstream_tasks=[delete_raw])


    # 5) Marca quais slices chegaram até o fim sem erro
    slice_completed = mark_slice_completed.map(delete_prepared)    

    # 6) Prepara DF de log de validação de finalização de sucesso da run
    df_validacao = validate_upload(
        run_id=run_id,
        as_of=as_of,
        environment=ENVIRONMENT,
        bq_table=BQ_TABLE,
        bq_dataset=BQ_DATASET,
        data_inicial=DATA_INICIAL,
        data_final=DATA_FINAL,
        slice_completed=slice_completed,
        upstream_tasks=[delete_prepared],
    )

    # 7) Registra a validação na tabela de log
    registra_validacao = upload_df_to_datalake(
        df=df_validacao,
        table_id=BQ_TABLE,
        dataset_id="brutos_sisreg_api_log",
        partition_column="as_of",
        source_format="parquet",
    )


# Configurações de execução
sms_sisreg_api.executor = LocalDaskExecutor(num_workers=10)
sms_sisreg_api.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_sisreg_api.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request=CONFIG["memory_request"],
    memory_limit=CONFIG["memory_limit"],
)
sms_sisreg_api.schedule = schedule
