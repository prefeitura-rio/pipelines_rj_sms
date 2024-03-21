# -*- coding: utf-8 -*-
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
#from pipelines.utils.credential_injector import authenticated_create_flow_run as create_flow_run
#from pipelines.utils.credential_injector import authenticated_wait_for_flow_run as wait_for_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.std.extracao.constants import constants as smsrio_constants
from pipelines.prontuarios.std.extracao.smsrio.tasks import get_params
from pipelines.prontuarios.std.extracao.utils import get_datetime_in_range
from pipelines.utils.tasks import get_secret_key, inject_gcp_credentials

with Flow(
    name="Prontuários (SMSRio e Vitai) - Padronização de Pacientes (carga histórica)",
) as smsrio_standardization_historical_all:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    START_DATETIME = Parameter(
        "source_start_datetime", default="2020-01-01 12:00:00", required=False
    )
    END_DATETIME = Parameter("source_end_datetime", default="2024-12-06 12:04:00", required=False)
    # RENAME_FLOW = Parameter("rename_flow", default=False)

    ####################################
    # Set environment
    ####################################

    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    DATABASE = get_secret_key(
        secret_path="/",
        secret_name=smsrio_constants.INFISICAL_DATABASE.value,
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection],
    )

    USER = get_secret_key(
        secret_path="/",
        secret_name=smsrio_constants.INFISICAL_DB_USER.value,
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection],
    )

    PASSWORD = get_secret_key(
        secret_path="/",
        secret_name=smsrio_constants.INFISICAL_DB_PASSWORD.value,
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection],
    )

    IP = get_secret_key(
        secret_path="/",
        secret_name=smsrio_constants.INFISICAL_IP.value,
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection],
    )

    request_params = get_params(START_DATETIME, END_DATETIME, upstream_tasks=[credential_injection])

    datetime_range_list_smsrio = get_datetime_in_range(
        USER=USER,
        PASSWORD=PASSWORD,
        DATABASE=DATABASE,
        IP=IP,
        ENVIROMENT=ENVIRONMENT,
        request_params=request_params,
        system="smsrio",
        upstream_tasks=[credential_injection],
    )

    datetime_range_list_vitai = get_datetime_in_range(
        USER=USER,
        PASSWORD=PASSWORD,
        DATABASE=DATABASE,
        IP=IP,
        ENVIROMENT=ENVIRONMENT,
        request_params=request_params,
        system="vitai",
        upstream_tasks=[credential_injection],
    )

    flow_smsrio = create_flow_run.map(
        flow_name=unmapped("Prontuários (SMSRio) - Padronização de carga histórica de pacientes"),
        # mudar se decidirmos levar o codigo para main
        project_name=unmapped("staging"),
        parameters=datetime_range_list_smsrio,
        upstream_tasks=[unmapped(credential_injection)],
    )

    wait_for_flow_smsrio = wait_for_flow_run.map(
        flow_smsrio,
        raise_final_state=unmapped(True),
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        upstream_tasks=[unmapped(credential_injection)],
    )

    flow_vitai = create_flow_run.map(
        flow_name=unmapped("Prontuários (Vitai) - Padronização de carga histórica de pacientes"),
        project_name=unmapped("staging"),
        parameters=datetime_range_list_vitai,
        upstream_tasks=[unmapped(credential_injection)],
    )

    wait_for_flow_vitai = wait_for_flow_run.map(
        flow_vitai,
        raise_final_state=unmapped(True),
        stream_states=unmapped(True),
        stream_logs=unmapped(True),
        upstream_tasks=[unmapped(credential_injection)],
    )

    flow_vitai.set_upstream(wait_for_flow_smsrio)

smsrio_standardization_historical_all.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smsrio_standardization_historical_all.executor = LocalDaskExecutor(num_workers=4)
smsrio_standardization_historical_all.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="8Gi",
)
