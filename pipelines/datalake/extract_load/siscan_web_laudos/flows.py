# -*- coding: utf-8 -*-
"""
Fluxo
"""

from datetime import datetime

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.siscan_web_laudos.constants import CONFIG
from pipelines.datalake.extract_load.siscan_web_laudos.schedules import schedule
from pipelines.datalake.extract_load.siscan_web_laudos.tasks import (
    build_operator_parameters,
    check_records,
    generate_extraction_windows,
    parse_date,
    run_siscan_scraper,
)
from pipelines.datalake.utils.tasks import (
    delete_file,
    extrair_fim,
    extrair_inicio,
    prepare_df_from_disk,
    rename_current_flow_run,
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
from pipelines.utils.time import from_relative_date

with Flow(
    name="SUBGERAL - Extract & Load - SISCAN WEB - Laudos (Operator)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
        constants.HERIAN_ID.value,
    ],
) as sms_siscan_web_operator:

    ###########################
    # Parâmetros
    ###########################
    ENVIRONMENT = Parameter("environment", default="dev")

    # PARAMETROS CREDENCIAIS ------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="SISCAN_MAIL", secret_path="/siscan_web"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="SISCAN_PASS", secret_path="/siscan_web"
    )

    # PARAMETROS CONSULTA ---------------------------
    OPCAO_EXAME = Parameter("opcao_exame", default="mamografia")
    DATA_INICIAL = Parameter("data_inicial", default="01/01/2025")
    DATA_FINAL = Parameter("data_final", default="31/01/2025")

    # PARAMETROS BQ ----------------------------------
    BQ_DATASET = Parameter("bq_dataset", default="brutos_siscan_web")
    BQ_TABLE = Parameter("bq_table", default="laudos_mamografia")

    ###########################
    # Flow
    ###########################
    # 1) Extrai e salva cada lote em disco, retorna caminho do parquet
    raw_files = run_siscan_scraper(
        email=user,
        password=password,
        opcao_exame=OPCAO_EXAME,
        start_date=DATA_INICIAL,
        end_date=DATA_FINAL,
        output_dir=CONFIG["output_dir"],
    )

    any_records = check_records(file_path=raw_files)

    with case(task=any_records, value=True):
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
    delete_prepared = delete_file(file_path=prepared_files, upstream_tasks=[delete_raw])


with Flow(
    name="SUBGERAL - Extract & Load - SISCAN WEB - Laudos (Manager)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
        constants.HERIAN_ID.value,
    ],
) as sms_siscan_web_manager:

    ###########################
    # Parâmetros
    ###########################
    ENVIRONMENT = Parameter("environment", default="dev")
    RELATIVE_DATE = Parameter("relative_date", default="D-1")
    DIAS_POR_FAIXA = Parameter("range", default=1)
    RENAME_FLOW = Parameter("rename_flow", default=True)
    OPCAO_EXAME = Parameter("opcao_exame", default="mamografia")
    BQ_DATASET = Parameter("bq_dataset", default="brutos_siscan_web")
    BQ_TABLE = Parameter("bq_table", default="laudos_mamografia")

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            relative_date=RELATIVE_DATE,
            range=DIAS_POR_FAIXA,
        )

    ###########################
    # Flow
    ###########################

    # Gera data relativa e a data atual
    relative_date = from_relative_date(relative_date=RELATIVE_DATE)

    # Gera as janelas de extração com base no interval
    windows = generate_extraction_windows(
        start_date=relative_date, end_date="", interval=DIAS_POR_FAIXA
    )

    interval_starts = extrair_inicio.map(faixa=windows)
    interval_ends = extrair_fim.map(faixa=windows)

    # Monta os parâmetros de cada operator
    prefect_project_name = get_project_name(environment=ENVIRONMENT)
    current_labels = get_current_flow_labels()

    operator_params = build_operator_parameters(
        start_dates=interval_starts,
        end_dates=interval_ends,
        environment=ENVIRONMENT,
        bq_dataset=BQ_DATASET,
        bq_table=BQ_TABLE,
        opcao_exame=OPCAO_EXAME,
    )
    # Cria e espera a execução das flow runs
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

with Flow(
    name="SUBGERAL - Extract & Load - SISCAN WEB - Laudos - Histórico",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
        constants.HERIAN_ID.value,
    ],
) as sms_siscan_web_historical:

    ###########################
    # Parâmetros
    ###########################
    ENVIRONMENT = Parameter("environment", default="dev")
    DIAS_POR_FAIXA = Parameter("range", default=1)
    RENAME_FLOW = Parameter("rename_flow", default=True)
    START_DATE = Parameter("start_date", default="01/01/2025")
    END_DATE = Parameter("end_date", default="31/01/2025")
    OPCAO_EXAME = Parameter("opcao_exame", default="mamografia")
    BQ_DATASET = Parameter("bq_dataset", default="brutos_siscan_web")
    BQ_TABLE = Parameter("bq_table", default="laudos_mamografia")

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            start_date=START_DATE,
            end_date=END_DATE,
            range=DIAS_POR_FAIXA,
        )

    ###########################
    # Flow
    ###########################

    start_date = parse_date(date=START_DATE)
    end_date = parse_date(date=END_DATE)

    # Gera as janelas de extração com base no interval
    windows = generate_extraction_windows(
        start_date=start_date, end_date=end_date, interval=DIAS_POR_FAIXA
    )
    interval_starts = extrair_inicio.map(faixa=windows)
    interval_ends = extrair_fim.map(faixa=windows)

    # Monta os parâmetros de cada operator
    prefect_project_name = get_project_name(environment=ENVIRONMENT)
    current_labels = get_current_flow_labels()

    operator_params = build_operator_parameters(
        start_dates=interval_starts,
        end_dates=interval_ends,
        environment=ENVIRONMENT,
        bq_dataset=BQ_DATASET,
        bq_table=BQ_TABLE,
        opcao_exame=OPCAO_EXAME,
    )

    # Cria e espera a execução das flow runs
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


###########################
# Configurações
###########################

# Operator
sms_siscan_web_operator.executor = LocalDaskExecutor(num_workers=1)
sms_siscan_web_operator.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_siscan_web_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="1Gi",
    memory_limit="1Gi",
)

# Manager
sms_siscan_web_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_siscan_web_manager.executor = LocalDaskExecutor(num_workers=6)
sms_siscan_web_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="2Gi",
    memory_request="2Gi",
)
sms_siscan_web_manager.schedule = schedule

# Histórico
sms_siscan_web_historical.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_siscan_web_historical.executor = LocalDaskExecutor(num_workers=6)
sms_siscan_web_historical.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="2Gi",
    memory_request="2Gi",
)
