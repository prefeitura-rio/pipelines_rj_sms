from schedules import flow_params
from constants import (
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    SENDER_EMAIL,
    SENDER_NAME,
)

from tasks import send_email_smtp

params_mapping = flow_params or {}
recipients = params_mapping.get("recipients", [])
subject = params_mapping.get("subject", "")
html_body = params_mapping.get("html_body", "")
plain_body = params_mapping.get("plain_body")
attachments = params_mapping.get("attachments")
inline_images = params_mapping.get("inline_images")

send_email_smtp(
    smtp_host=SMTP_HOST,
    smtp_port=SMTP_PORT,
    smtp_user=SMTP_USER,
    smtp_password=SMTP_PASSWORD,
    sender_email=SENDER_EMAIL,
    sender_name=SENDER_NAME,
    recipients=recipients,
    subject=subject,
    html_body=html_body,
    plain_body=plain_body,
    attachments=attachments,
    inline_images=inline_images,
    use_ssl=False,
)
