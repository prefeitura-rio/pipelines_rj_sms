# -*- coding: utf-8 -*-
"""
Fluxo
"""
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.reports.emails_subgeral_gestao.constants import (
    GSHEETS_URL,
    SENDER_NAME,
    SMTP_HOST,
    SMTP_PORT,
)
from pipelines.reports.emails_subgeral_gestao.schedules import schedule
from pipelines.reports.emails_subgeral_gestao.tasks import (
    bigquery_to_xl_payload,
    get_recipients_from_gsheets,
    send_email_smtp,
)
from pipelines.reports.utils.emails_subgeral import (
    delete_file_from_disk,
    make_email_meta_df,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import (
    get_secret_key,
    inject_gcp_credentials,
    upload_df_to_datalake,
)

with Flow(
    name="SUBGERAL - Envia E-mails",
    state_handlers=[handle_flow_state_change],
    owners=[constants.MATHEUS_ID.value],
) as sms_emails_subgeral:

    # PARAMETROS AMBIENTE ---------------------------
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # CREDENCIAIS ------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="MAILGRID_USER", secret_path="/emails_subgeral"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="MAILGRID_PASSWORD", secret_path="/emails_subgeral"
    )

    gcp_credentials = inject_gcp_credentials(environment=ENVIRONMENT)

    # PARAMETROS GSHEETS ---------------------------
    GSHEETS_SHEET_NAME = Parameter("gsheets_sheet_name", default=None)

    # PARAMETROS EMAIL ------------------------------
    SUBJECT = Parameter("subject", default=None)
    QUERY_PATH = Parameter("query_path", default=None)
    HTML_BODY_PATH = Parameter("html_body_path", default=None)
    PLAIN_BODY_PATH = Parameter("plain_body_path", default=None)

    # Task 0 - Obtém lista de emails do Google Sheets
    recipients = get_recipients_from_gsheets(
        gsheets_url=GSHEETS_URL, gsheets_sheet_name=GSHEETS_SHEET_NAME
    )

    # Task 1 - Obtém dados, salva no disco e retorna path absoluto
    xl_payload = bigquery_to_xl_payload(subject=SUBJECT, query_path=QUERY_PATH)

    # Task 2 - Envia o email
    email = send_email_smtp(
        smtp_host=SMTP_HOST,
        smtp_port=SMTP_PORT,
        smtp_user=user,
        smtp_password=password,
        sender_email=user,
        sender_name=SENDER_NAME,
        recipients=recipients,
        subject=SUBJECT,
        html_body_path=HTML_BODY_PATH,
        plain_body_path=PLAIN_BODY_PATH,
        attachments=xl_payload,
        use_ssl=False,
        upstream_tasks=[recipients],
    )

    # Task 3 - Cria metadados sobre emails enviados
    df = make_email_meta_df(
        environment=ENVIRONMENT,
        subject=SUBJECT,
        recipients=recipients,
        query_path=QUERY_PATH,
        attachments=xl_payload,
        html_body_path=HTML_BODY_PATH,
        plain_body_path=PLAIN_BODY_PATH,
        sender_name=SENDER_NAME,
        sender_email=user,
        smtp_user=user,
        smtp_host=SMTP_HOST,
        smtp_port=SMTP_PORT,
    )

    # Task 4 - Escreve metadados no Big Query
    write_meta = upload_df_to_datalake(
        df=df,
        table_id="logs_emails_gestao",
        dataset_id="brutos_emails_subgeral",
        partition_column="date_partition",
        source_format="parquet",
    )

sms_emails_subgeral.executor = LocalDaskExecutor(num_workers=1)
sms_emails_subgeral.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_emails_subgeral.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="10Gi",
    memory_limit="10Gi",
)
sms_emails_subgeral.schedule = schedule
