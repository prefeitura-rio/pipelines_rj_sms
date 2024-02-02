# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow for SMSRio Raw Data Extraction
"""
from prefect import case
from prefect import Parameter
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.utils.tasks import (
    inject_gcp_credentials,
)
from pipelines.prontuarios.std.vitai.tasks import (
    load_from_api,
    load_to_api,
    get_database_url,
    standartize_data
)

from pipelines.prontuarios.std.vitai.constants import constants as smsrio_constants

from pipelines.prontuarios.utils.tasks import (
    get_api_token,
    rename_current_flow_run
)

with Flow(
    name="Prontuários (SMSRio)- Puxando dados da API (teste)",
) as sms_prontuarios_standartized_smsrio:

    ENVIRONMENT = Parameter("environment", default="dev")

    RENAME_FLOW = Parameter("rename_flow", default=False)

    credential_injection = inject_gcp_credentials(environment=ENVIRONMENT)

    database_url = get_database_url(
        environment=ENVIRONMENT,
        upstream_tasks=[credential_injection]
    )

    api_token = get_api_token(
        environment=ENVIRONMENT,
        infisical_path=smsrio_constants.INFISICAL_PATH.value,
        infisical_api_username=smsrio_constants.INFISICAL_API_USERNAME.value,
        infisical_api_password=smsrio_constants.INFISICAL_API_PASSWORD.value,
        upstream_tasks=[credential_injection]
    )

    with case(RENAME_FLOW, True):
        rename_flow_task = rename_current_flow_run(
            environment=ENVIRONMENT,
            cnes=smsrio_constants.SMSRIO_CNES.value,
            upstream_tasks=[credential_injection]
        )

    ####################################
    # Task Section #1 - Get data
    ####################################

    data_api = load_from_api(
        upstream_tasks=[credential_injection],
        params={'start_datetime': "2024-02-01T13:00:00.000000",
                'end_datetime': "2024-02-01T14:00:00.000000",
                'datasource_system': 'vitai'},
        endpoint_name='raw/patientrecords',
        api_token=api_token,
        environment=ENVIRONMENT
    )

    ####################################
    # Task Section #2 - Transforming data
    ####################################

    std_data = standartize_data(
        upstream_tasks=[credential_injection],
        data=data_api
    )

    ####################################
    # Task Section #1 - Loading data to API
    ####################################

    load_to_api_task = load_to_api(
        upstream_tasks=[credential_injection],
        request_body=std_data,
        endpoint_name="std/patientrecords",
        api_token=api_token,
        environment=ENVIRONMENT

    )
