# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.cientificalab_api.constants import (
    cientificalab_constants,
)
from pipelines.datalake.extract_load.cientificalab_api.schedules import schedule
from pipelines.datalake.extract_load.cientificalab_api.tasks import (
    authenticate_and_fetch,
    build_operator_params,
    generate_daily_windows,
    get_identificador_lis,
    transform,
)
from pipelines.datalake.utils.tasks import upload_df_to_datalake
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
from pipelines.utils.time import from_relative_date

with Flow(
    name="DataLake - Extração e Carga de Dados - CientificaLab (Operator)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DANIEL_ID.value,
    ],
) as flow_cientificalab_operator:
    environment = Parameter("environment", default="dev")
    dt_inicio = Parameter("dt_inicio", default="2025-01-21T10:00:00-0300")
    dt_fim = Parameter("dt_fim", default="2025-01-21T11:30:00-0300")
    cnes = Parameter("cnes", required=True)
    rename_flow = Parameter("rename_flow", default=True)
    dataset_id = Parameter(
        "dataset", default=cientificalab_constants.DATASET_ID.value, required=False
    )  # noqa

    # INFISICAL
    INFISICAL_PATH = cientificalab_constants.INFISICAL_PATH.value

    username_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=cientificalab_constants.INFISICAL_USERNAME.value,
        environment=environment,
    )
    password_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=cientificalab_constants.INFISICAL_PASSWORD.value,
        environment=environment,
    )
    apccodigo_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=cientificalab_constants.INFISICAL_APCCODIGO.value,
        environment=environment,
    )
    codigo_lis_secret = get_secret_key(
        secret_path=INFISICAL_PATH,
        secret_name=cientificalab_constants.INFISICAL_CODIGOLIS.value,
        environment=environment,
    )

    with case(rename_flow, True):
        rename_current_flow_run(
            name_template="cnes: {cnes} dt_ini: {dt_inicio} dt_fim: {dt_fim} ({environment})",
            cnes=cnes,
            dt_inicio=dt_inicio,
            dt_fim=dt_fim,
            environment=environment,
        )

    identificador_lis = get_identificador_lis(secret_json=codigo_lis_secret, cnes=cnes)

    results = authenticate_and_fetch(
        username=username_secret,
        password=password_secret,
        apccodigo=apccodigo_secret,
        identificador_lis=identificador_lis,
        dt_start=dt_inicio,
        dt_end=dt_fim,
        environment=environment,
    )

    solicitacoes_df, exames_df, resultados_df = transform(json_result=results)

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
    "DataLake - Extração e Carga de Dados - CientificaLab (Manager)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DANIEL_ID.value,
    ],
) as flow_cientificalab_manager:

    environment = Parameter("environment", default="dev")
    relative_date_filter = Parameter("intervalo", default="D-1")

    prefect_project_name = get_project_name(environment=environment)
    current_labels = get_current_flow_labels()

    cnes_list = cientificalab_constants.CNES.value

    start_date = from_relative_date(relative_date=relative_date_filter)

    windows = generate_daily_windows(start_date=start_date)

    operator_parameters = build_operator_params(
        windows=windows, env=environment, cnes_list=cnes_list
    )

    created_operator_runs = create_flow_run.map(
        flow_name=unmapped(flow_cientificalab_operator.name),
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

flow_cientificalab_operator.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cientificalab_operator.executor = LocalDaskExecutor(num_workers=3)
flow_cientificalab_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="6Gi",
)

flow_cientificalab_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cientificalab_manager.executor = LocalDaskExecutor(num_workers=6)
flow_cientificalab_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

flow_cientificalab_manager.schedule = schedule
