# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants

from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key

# from .schedules import schedule
from .tasks import (
    build_email,
    fetch_cids,
    get_email_recipients,
    send_email
)
from .constants import informes_seguranca_constants

with Flow(
    name="Report: Informes de Segurança",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.AVELLAR_ID.value,
    ],
) as report_informes_seguranca:

    #####################################
    # Parameters
    #####################################
    ENVIRONMENT = Parameter("environment")
    DATE = Parameter("date", default=None)
    OVERRIDE_RECIPIENTS = Parameter("override_recipients", default=None)

    #####################################
    # Tasks
    #####################################

    results = fetch_cids(
        environment=ENVIRONMENT,
        date=DATE
    )

    message = build_email(cids=results)

    recipients = get_email_recipients(recipients=OVERRIDE_RECIPIENTS)

    URL = get_secret_key(
        secret_path=informes_seguranca_constants.EMAIL_PATH.value,
        secret_name=informes_seguranca_constants.EMAIL_URL.value,
        environment=ENVIRONMENT,
    )
    TOKEN = get_secret_key(
        secret_path=informes_seguranca_constants.EMAIL_PATH.value,
        secret_name=informes_seguranca_constants.EMAIL_TOKEN.value,
        environment=ENVIRONMENT,
    )
    send_email(api_base_url=URL, token=TOKEN, message=message, recipients=recipients)

## TODO: schedules; task retries; email

# report_informes_seguranca.schedule = schedule
report_informes_seguranca.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_informes_seguranca.executor = LocalDaskExecutor(num_workers=1)
report_informes_seguranca.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
