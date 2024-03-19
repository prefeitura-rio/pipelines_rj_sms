# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.std.extracao.constants import constants as smsrio_constants
from pipelines.prontuarios.std.extracao.smsrio.tasks import get_params
from pipelines.prontuarios.std.extracao.utils import (
    get_datetime_in_range,
    run_flow_smsrio,
    run_flow_vitai,
)
from pipelines.utils.tasks import get_secret_key

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

    DATABASE = get_secret_key(
        secret_path="/",
        secret_name=smsrio_constants.INFISICAL_DATABASE.value,
        environment=ENVIRONMENT,
    )

    USER = get_secret_key(
        secret_path="/",
        secret_name=smsrio_constants.INFISICAL_DB_USER.value,
        environment=ENVIRONMENT,
    )

    PASSWORD = get_secret_key(
        secret_path="/",
        secret_name=smsrio_constants.INFISICAL_DB_PASSWORD.value,
        environment=ENVIRONMENT,
    )

    IP = get_secret_key(
        secret_path="/", secret_name=smsrio_constants.INFISICAL_IP.value, environment=ENVIRONMENT
    )

    request_params = get_params(START_DATETIME, END_DATETIME)

    datetime_range_list_smsrio = get_datetime_in_range(
        USER=USER,
        PASSWORD=PASSWORD,
        DATABASE=DATABASE,
        IP=IP,
        request_params=request_params,
        system="smsrio",
    )

    run = run_flow_smsrio(
        datetime_range_list=datetime_range_list_smsrio,
        DATABASE=DATABASE,
        USER=USER,
        PASSWORD=PASSWORD,
        IP=IP,
    )

    datetime_range_list_vitai = get_datetime_in_range(
        USER=USER,
        PASSWORD=PASSWORD,
        DATABASE=DATABASE,
        IP=IP,
        request_params=request_params,
        system="vitai",
    )

    run_flow_vitai(
        datetime_range_list=datetime_range_list_vitai,
        DATABASE=DATABASE,
        USER=USER,
        PASSWORD=PASSWORD,
        IP=IP,
        run=run,
    )


smsrio_standardization_historical_all.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
smsrio_standardization_historical_all.executor = LocalDaskExecutor(num_workers=4)
smsrio_standardization_historical_all.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="8Gi",
)
