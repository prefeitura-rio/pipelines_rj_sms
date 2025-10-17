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
from pipelines.utils.tasks import OR, get_email_recipients, get_secret_key

from .constants import informes_seguranca_constants
from .schedules import schedule
from .tasks import build_email, fetch_cids, send_email

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
    ENVIRONMENT = Parameter("environment", default="dev")
    DATE = Parameter("date", default=None)
    OVERRIDE_RECIPIENTS = Parameter("override_recipients", default=None)
    WRITE_FILE_SKIP_EMAIL = Parameter("write_file_skip_email", default=False)

    #####################################
    # Tasks
    #####################################

    results, fetch_error = fetch_cids(environment=ENVIRONMENT, date=DATE)

    message, build_error = build_email(cids=results, date=DATE, error=fetch_error)

    has_error = OR(a=fetch_error, b=build_error)

    recipients = get_email_recipients(
        environment=ENVIRONMENT,
        dataset="brutos_sheets",
        table="seguranca_destinatarios",
        recipients=OVERRIDE_RECIPIENTS,
        error=has_error,
    )

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
    send_email(
        api_base_url=URL,
        token=TOKEN,
        message=message,
        recipients=recipients,
        error=has_error,
        date=DATE,
        write_to_file_instead=WRITE_FILE_SKIP_EMAIL,
    )


report_informes_seguranca.schedule = schedule
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
