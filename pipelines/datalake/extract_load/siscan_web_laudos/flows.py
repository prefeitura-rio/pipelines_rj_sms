# -*- coding: utf-8 -*-
"""
Fluxo
"""

# TO DO: PARALELIZAR

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.siscan_web_laudos.constants import CONFIG
from pipelines.datalake.extract_load.siscan_web_laudos.schedules import schedule
from pipelines.datalake.extract_load.siscan_web_laudos.tasks import run_siscan_scraper
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
from pipelines.utils.tasks import get_secret_key

with Flow(
    name="SUBGERAL - Extract & Load - SISCAN WEB - Laudos",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
    ],
) as sms_siscan_web:
    # PARAMETROS AMBIENTE ---------------------------
    ENVIRONMENT = Parameter("environment", default="dev")

    # PARAMETROS CREDENCIAIS ------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="SISCAN_MAIL", secret_path="/siscan_web"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="SISCAN_PASS", secret_path="/siscan_web"
    )

    # PARAMETROS CONSULTA ---------------------------
    DATA_INICIAL = Parameter("data_inicial", default="01/01/2025")
    DATA_FINAL = Parameter("data_final", default="31/01/2025")

    # PARAMETROS PARA DEFINIR TAMANHO DOS LOTES ------
    DIAS_POR_FAIXA = Parameter("dias_por_faixa", default=10)
    FORMATO_DATA = Parameter("formato_data", default="%d/%m/%Y")

    # PARAMETROS BQ ----------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_siscan_web")
    BQ_TABLE = Parameter("bq_table", default="laudos")

    faixas = gerar_faixas_de_data(
        data_inicial=DATA_INICIAL,
        data_final=DATA_FINAL,
        dias_por_faixa=DIAS_POR_FAIXA,
        date_format=FORMATO_DATA,
    )

    inicio_faixas = extrair_inicio.map(faixa=faixas)
    fim_faixas = extrair_fim.map(faixa=faixas)

    # 1) Extrai e salva cada lote em disco, retorna caminho do parquet
    raw_files = run_siscan_scraper.map(
        email=unmapped(user),
        password=unmapped(password),
        start_date=inicio_faixas,
        end_date=fim_faixas,
        output_dir=unmapped(CONFIG["output_dir"]),
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
    delete_prepared = delete_file.map(file_path=prepared_files, upstream_tasks=[uploads])

# Configurações de execução
sms_siscan_web.executor = LocalDaskExecutor(num_workers=3)
sms_siscan_web.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_siscan_web.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request=CONFIG["memory_request"],
    memory_limit=CONFIG["memory_limit"],
)
sms_siscan_web.schedule = schedule
