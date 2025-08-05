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
from pipelines.datalake.extract_load.siscan_web_laudos.tasks import (
    build_operator_parameters,
    run_siscan_scraper,
)
from pipelines.datalake.utils.tasks import (
    delete_file,
    extrair_fim,
    extrair_inicio,
    gerar_faixas_de_data,
    prepare_df_from_disk,
    upload_from_disk,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)

# internos
from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_project_name, get_secret_key
from pipelines.utils.time import from_relative_date, get_datetime_working_range

with Flow(
    name="SUBGERAL - Extract & Load - SISCAN WEB - Laudos (Operator)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
        constants.HERIAN_ID.value,
    ],
) as sms_siscan_web_operator:
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

    # PARAMETROS BQ ----------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_siscan_web")
    BQ_TABLE = Parameter("bq_table", default="laudos")

    # 1) Extrai e salva cada lote em disco, retorna caminho do parquet
    raw_files = run_siscan_scraper(
        email=user,
        password=password,
        start_date=DATA_INICIAL,
        end_date=DATA_FINAL,
        output_dir=CONFIG["output_dir"],
    )

    # 2) Prepara cada arquivo (lê e gera outro parquet)
    prepared_files = prepare_df_from_disk(
        file_path=raw_files,
        flow_name=CONFIG["flow_name"],
        flow_owner=CONFIG["flow_owner"],
    )

    # 3) Faz o upload lendo cada arquivo preparado do disco
    uploads = upload_from_disk(
        file_path=prepared_files,
        table_id=BQ_TABLE,
        dataset_id=BQ_DATASET,
        partition_column=CONFIG["partition_column"],
        source_format=CONFIG["source_format"],
    )

    # 4) Exclui os arquivos após o upload
    delete_raw = delete_file(file_path=raw_files, upstream_tasks=[uploads])
    delete_prepared = delete_file(file_path=prepared_files, upstream_tasks=[uploads])


with Flow(
    name="SUBGERAL - Extract & Load - SISCAN WEB - Laudos (Manager)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
        constants.HERIAN_ID.value,
    ],
) as sms_siscan_web_manager:
    # Parâmetros
    ENVIRONMENT = Parameter("environment", default="dev")
    RELATIVE_DATE = Parameter("relative_date", default="D-1")
    DIAS_POR_FAIXA = Parameter("range", default=7)

    data_filter = from_relative_date(relative_date=RELATIVE_DATE)
    start_date, end_date = get_datetime_working_range(start_datetime=data_filter)

    faixas = gerar_faixas_de_data(
        data_inicial=start_date,
        data_final=end_date,
        dias_por_faixa=DIAS_POR_FAIXA,
    )

    inicio_faixas = extrair_inicio.map(faixa=faixas)
    fim_faixas = extrair_fim.map(faixa=faixas)
    prefect_project_name = get_project_name(environment=ENVIRONMENT)
    current_labels = get_current_flow_labels()

    operator_params = build_operator_parameters(
        start_dates=inicio_faixas,
        end_dates=fim_faixas,
        environment=ENVIRONMENT,
    )

    created_operator_runs = create_flow_run.map(
        flow_name=unmapped(sms_siscan_web_operator.name),
        project_name=unmapped(prefect_project_name),
        labels=unmapped(current_labels),
        parameters=operator_params,
        run_name=unmapped(None),
    )

    wait_for_operator_runs = wait_for_flow_run.map(
        flow_run_id=created_operator_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(True),
    )

# Configurações de execução
sms_siscan_web_operator.executor = LocalDaskExecutor(num_workers=3)
sms_siscan_web_operator.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_siscan_web_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request=CONFIG["memory_request"],
    memory_limit=CONFIG["memory_limit"],
)
sms_siscan_web_operator.schedule = schedule


sms_siscan_web_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_siscan_web_manager.executor = LocalDaskExecutor(num_workers=6)
sms_siscan_web_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="2Gi",
    memory_request="2Gi",
)
