# -*- coding: utf-8 -*-
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.cientificalab_api.constants import (
    constants as cientificalab_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.datalake.extract_load.cientificalab_api.schedules import schedule
from pipelines.datalake.extract_load.cientificalab_api.tasks import (
    authenticate_and_fetch,
    transform,
    generate_extraction_windows,
)
from pipelines.utils.credential_injector import (
    authenticated_create_flow_run as create_flow_run,
    authenticated_wait_for_flow_run as wait_for_flow_run,
)

from pipelines.utils.flow import Flow
from pipelines.utils.prefect import get_current_flow_labels
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake
from pipelines.utils.time import get_datetime_working_range, from_relative_date

with Flow(
    name="DataLake - Extração e Carga de Dados - CientificaLab (Operator)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DANIEL_ID.value,
    ],
) as flow_cientificalab:
    ENVIRONMENT = Parameter("environment", default="dev")
    DT_INICIO = Parameter("dt_inicio", default="2025-01-21T10:00:00-0300")
    DT_FIM = Parameter("dt_fim", default="2025-01-21T11:30:00-0300")

    # INFISICAL
    INFISICAL_PATH = cientificalab_constants.INFISICAL_PATH.value
    INFISICAL_USERNAME = cientificalab_constants.INFISICAL_USERNAME.value
    INFISICAL_PASSWORD = cientificalab_constants.INFISICAL_PASSWORD.value
    INFISICAL_APCCODIGO = cientificalab_constants.INFISICAL_APCCODIGO.value

    username_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_USERNAME, environment=ENVIRONMENT
    )
    password_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_PASSWORD, environment=ENVIRONMENT
    )
    apccodigo_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_APCCODIGO, environment=ENVIRONMENT
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

    resultado_xml = authenticate_and_fetch(
        username=username_secret,
        password=password_secret,
        apccodigo=apccodigo_secret,
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

flow_cientificalab.schedule = schedule
flow_cientificalab.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cientificalab.executor = LocalDaskExecutor(num_workers=3)
flow_cientificalab.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)

with Flow(
    "DataLake - CientificaLab Historic - Manager",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DANIEL_ID.value],
) as flow_cientificalab_manager:


    environment = Parameter("environment", default="staging", required=True)
    relative_date = Parameter("RELATIVE_DATE_FILTER", default="M-1", required=True)


    start_date = from_relative_date(relative_date=relative_date)


    all_windows = generate_extraction_windows(start_date=start_date)


    @task
    def build_operator_params(windows: list, env: str) -> list:
        return [{"dt_inicio": w["dt_inicio"], "dt_fim": w["dt_fim"], "environment": env} for w in windows]

    operator_parameters = build_operator_params(windows=all_windows, env=environment)


    project_name = constants.GCP_PROJECT_ID.value
    current_labels = get_current_flow_labels()

    created_runs = create_flow_run.map(
        flow_name=unmapped(flow_cientificalab.name),
        project_name=unmapped(project_name),
        parameters=operator_parameters,
        labels=unmapped(current_labels),
    )

    wait_for_runs = wait_for_flow_run.map(
        flow_run_id=created_runs,
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        raise_final_state=unmapped(False),
    )


flow_cientificalab_manager.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cientificalab_manager.executor = LocalDaskExecutor(num_workers=1)
flow_cientificalab_manager.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="2Gi",
    memory_request="1Gi",
)