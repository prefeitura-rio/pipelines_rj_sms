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
    generate_extraction_windows,
    get_identificador_lis,
    transform,
)
from pipelines.datalake.utils.tasks import (
    rename_current_flow_run,
    upload_df_to_datalake,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
)
from pipelines.utils.credential_injector import (
    authenticated_wait_for_flow_run as wait_for_flow_run,
)
from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_project_name, get_secret_key
from pipelines.utils.time import from_relative_date, get_datetime_working_range

with Flow(
    name="DataLake - Extração e Carga de Dados - CientificaLab (Operator)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DANIEL_ID.value,
    ],
) as flow_cientificalab_operator:
    ENVIRONMENT = Parameter("environment", default="dev")
    DT_INICIO = Parameter("dt_inicio", default="2025-01-21T10:00:00-0300")
    DT_FIM = Parameter("dt_fim", default="2025-01-21T11:30:00-0300")
    CNES = Parameter("cnes", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=True)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            cnes=CNES,
            dt_inicio=DT_INICIO,
            dt_fim=DT_FIM,
            environment=ENVIRONMENT,
        )

    # INFISICAL
    INFISICAL_PATH = cientificalab_constants.INFISICAL_PATH.value
    INFISICAL_USERNAME = cientificalab_constants.INFISICAL_USERNAME.value
    INFISICAL_PASSWORD = cientificalab_constants.INFISICAL_PASSWORD.value
    INFISICAL_APCCODIGO = cientificalab_constants.INFISICAL_APCCODIGO.value
    INFISICAL_CODIGOLIS = cientificalab_constants.INFISICAL_CODIGOLIS.value

    username_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_USERNAME, environment=ENVIRONMENT
    )
    password_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_PASSWORD, environment=ENVIRONMENT
    )
    apccodigo_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_APCCODIGO, environment=ENVIRONMENT
    )
    codigo_lis_secret = get_secret_key(
        secret_path=INFISICAL_PATH,secret_name=INFISICAL_CODIGOLIS, environment=ENVIRONMENT
    )

    # BIG QUERY
    DATASET_ID = Parameter(
        "dataset_id", default=cientificalab_constants.DATASET_ID.value, required=False
    )

    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=DT_INICIO,
        end_datetime=DT_FIM,
        interval=1,
        return_as_str=True,
        timezone="America/Sao_Paulo",
    )

    IDENTIFICADOR_LIS = get_identificador_lis(cnes_lis=codigo_lis_secret)

    resultado_xml = authenticate_and_fetch(
        username=username_secret,
        password=password_secret,
        apccodigo=apccodigo_secret,
        identificador_lis=IDENTIFICADOR_LIS,
        dt_inicio=start_datetime,
        dt_fim=end_datetime,
        environment=ENVIRONMENT,
    )

    solicitacoes_df, exames_df, resultados_df = transform(resultado_xml=resultado_xml)

    solicitacoes_upload_task = upload_df_to_datalake(
        df=solicitacoes_df,
        dataset_id=DATASET_ID,
        table_id="solicitacoes",
        source_format="parquet",
        partition_column="datalake_loaded_at",
    )
    exames_upload_task = upload_df_to_datalake(
        df=exames_df,
        dataset_id=DATASET_ID,
        table_id="exames",
        source_format="parquet",
        partition_column="datalake_loaded_at",
        upstream_tasks=[solicitacoes_upload_task],
    )
    resultados_upload_task = upload_df_to_datalake(
        df=resultados_df,
        dataset_id=DATASET_ID,
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
    relative_date_filter = Parameter("relative_date", default="D-1")

    cnes_list = cientificalab_constants.CNES.value

    prefect_project_name = get_project_name(environment=environment)

    current_labels = get_current_flow_labels()

    date_filter = from_relative_date(relative_date=relative_date_filter)

    windows = generate_extraction_windows(start_date=date_filter)

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
flow_cientificalab_operator.executor = LocalDaskExecutor(num_workers=4)
flow_cientificalab_operator.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)

flow_cientificalab_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cientificalab_manager.executor = LocalDaskExecutor(num_workers=4)
flow_cientificalab_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="2Gi",
)

flow_cientificalab_manager.schedule = schedule
