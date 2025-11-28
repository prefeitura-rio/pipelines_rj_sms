# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.exames_laboratoriais_api.schedules import schedule
from pipelines.datalake.extract_load.exames_laboratoriais_api.tasks import (
    authenticate_fetch,
    build_operator_params,
    generate_time_windows,
    get_all_aps,
    get_credential_param,
    get_source_from_ap,
    parse_identificador,
    transform,
)
from pipelines.datalake.utils.tasks import upload_df_to_datalake
from pipelines.utils.basics import from_relative_date
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_project_name,
    get_secret_key,
    rename_current_flow_run,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - Exames Laboratoriais (Operator)",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DANIEL_ID.value],
) as exames_laboratoriais_operator:

    ap = Parameter("ap", default="10")
    environment = Parameter("environment", default="dev")
    dt_inicio = Parameter("dt_inicio", default="2025-10-21T10:00:00-0300")
    dt_fim = Parameter("dt_fim", default="2025-10-21T11:30:00-0300")
    rename_flow = Parameter("rename_flow", default=True)
    dataset_id = Parameter("dataset", default="exames_laboratoriais", required=False)

    source = get_source_from_ap(ap=ap)

    credential = get_credential_param(source=source)

    INFISICAL_PATH = credential["INFISICAL_PATH"]

    username_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=credential["INFISICAL_USERNAME"],
        environment=environment,
    )

    password_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=credential["INFISICAL_PASSWORD"],
        environment=environment,
    )

    apccodigo_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=credential["INFISICAL_APCCODIGO"],
        environment=environment,
    )

    identificador_lis_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=credential["INFISICAL_AP_LIS"],
        environment=environment,
    )

    with case(rename_flow, True):
        rename_current_flow_run(
            name_template="(AP{ap}) INÍCIO: {dt_inicio} FIM: {dt_fim}",
            dt_inicio=dt_inicio,
            dt_fim=dt_fim,
            ap=ap,
        )

    identificador_lis = parse_identificador(identificador=identificador_lis_secret, ap=ap)

    # autenticação + busca
    results = authenticate_fetch(
        username=username_secret,
        password=password_secret,
        apccodigo=apccodigo_secret,
        identificador_lis=identificador_lis,
        dt_start=dt_inicio,
        dt_end=dt_fim,
        environment=environment,
        source=source,
    )

    # transformação
    solicitacoes_df, exames_df, resultados_df = transform(json_result=results, source=source)

    # carga no Datalake
    solicitacoes_upload_task = upload_df_to_datalake(
        df=solicitacoes_df,
        dataset_id=dataset_id,
        table_id="solicitacoes",
        source_format="parquet",
        partition_column="datalake_loaded_at",
    )

    exames_upload_task = upload_df_to_datalake(
        df=exames_df,
        dataset_id=dataset_id,
        table_id="exames",
        source_format="parquet",
        partition_column="datalake_loaded_at",
        upstream_tasks=[solicitacoes_upload_task],
    )

    resultados_upload_task = upload_df_to_datalake(
        df=resultados_df,
        dataset_id=dataset_id,
        table_id="resultados",
        source_format="parquet",
        partition_column="datalake_loaded_at",
        upstream_tasks=[exames_upload_task],
    )

with Flow(
    "DataLake - Extração e Carga de Dados - Exames Laboratoriais (Manager)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DANIEL_ID.value,
    ],
) as exames_laboratoriais_manager:

    dataset_id = Parameter("dataset", default="exames_laboratoriais", required=False)
    environment = Parameter("environment", default="dev")
    relative_date_filter = Parameter("intervalo", default="D-1")
    hours_per_window = Parameter("hours_per_window", default=2)
    end_date = Parameter("end_date", default=None)

    prefect_project_name = get_project_name(environment=environment)
    current_labels = get_current_flow_labels()

    start_date = from_relative_date(relative_date=relative_date_filter)
    windows = generate_time_windows(
        start_date=start_date, hours_per_window=hours_per_window, end_date=end_date
    )
    aps_list = get_all_aps()

    operator_parameters = build_operator_params(
        windows=windows, aps=aps_list, env=environment, dataset=dataset_id
    )

    created_operator_runs = create_flow_run.map(
        flow_name=unmapped(exames_laboratoriais_operator.name),
        project_name=unmapped(prefect_project_name),
        parameters=operator_parameters,
        labels=unmapped(current_labels),
        run_name=unmapped(None),
    )

    wait_for_operator_runs = wait_for_flow_run.map(
        flow_run_id=created_operator_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(False),
    )

exames_laboratoriais_operator.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
exames_laboratoriais_operator.executor = LocalDaskExecutor(num_workers=1)
exames_laboratoriais_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)

exames_laboratoriais_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
exames_laboratoriais_manager.executor = LocalDaskExecutor(num_workers=8)
exames_laboratoriais_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)

exames_laboratoriais_manager.schedule = schedule
