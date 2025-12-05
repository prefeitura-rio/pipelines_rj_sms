# -*- coding: utf-8 -*-
"""
Fluxo
"""
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.emails_subgeral.constants import (
    SMTP_HOST,
    SMTP_PORT,
    SENDER_NAME
)
from pipelines.datalake.extract_load.emails_subgeral.schedules import schedule
from pipelines.datalake.extract_load.emails_subgeral.tasks import (
    bigquery_to_xl_disk,
    send_email_smtp,
    delete_file_from_disk,
    make_meta_df
)

# internos
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

###

with Flow(
        name="SUBGERAL - Envia E-mails",
        state_handlers=[handle_flow_state_change],
        owners=[constants.MATHEUS_ID.value]
    ) as sms_emails_subgeral:

    # PARAMETROS AMBIENTE ---------------------------
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # PARAMETROS CREDENCIAIS ------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="MAILGRID_USER", secret_path="/emails_subgeral"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="MAILGRID_PASSWORD", secret_path="/emails_subgeral"
    )

    # PARAMETROS FLUXO ------------------------------
    SUBJECT = Parameter("subject", default=None)
    RECIPIENTS = Parameter("recipients", default=None)
    QUERY_PATH = Parameter("query_path", default=None)
    HTML_BODY_PATH = Parameter("html_body_path", default=None)
    PLAIN_BODY_PATH = Parameter("plain_body_path", default=None)



    # TASKS -----------------------------------------------       
    # Task 1 - Obtém dados, salva no disco e retorna path absoluto
    xl_absolute_path = bigquery_to_xl_disk(subject=SUBJECT, query_path=QUERY_PATH)

    # Task 2 - Envia o email
    email = send_email_smtp(
        smtp_host=SMTP_HOST,
        smtp_port=SMTP_PORT,
        smtp_user=user,
        smtp_password=password,
        sender_email=user,
        sender_name=SENDER_NAME,
        recipients=RECIPIENTS,
        subject=SUBJECT,
        html_body_path=HTML_BODY_PATH,
        plain_body_path=PLAIN_BODY_PATH,
        attachments=xl_absolute_path,
        use_ssl=False
    )

    # Task 3 - remove arquivo do disco
    delete_file = delete_file_from_disk(
        filepath=xl_absolute_path,
        upstream_tasks=[email])

    # Task 4 - Cria metadados sobre emails enviados
    df = make_meta_df(
        environment = ENVIRONMENT,
        subject = SUBJECT,
        recipients = RECIPIENTS,
        query_path = QUERY_PATH,
        attachments = xl_absolute_path,
        html_body_path = HTML_BODY_PATH,
        plain_body_path = PLAIN_BODY_PATH,
        sender_name = SENDER_NAME,
        sender_email = user,
        smtp_user = user,
        smtp_host = SMTP_HOST,
        smtp_port = SMTP_PORT,
        upstream_tasks=[delete_file]
    )

    # Task 5 - Escreve metadados no Big Query
    write_meta = upload_df_to_datalake(
        df=df,
        table_id="logs",
        dataset_id="brutos_emails_subgeral",
        partition_column="as_of",
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
