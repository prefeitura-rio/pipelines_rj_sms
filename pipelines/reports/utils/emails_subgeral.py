# -*- coding: utf-8 -*-
import re
import smtplib
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import List, Sequence, Type

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.reports.emails_subgeral_gestao.constants import BASE_DIR
from pipelines.utils.credential_injector import authenticated_task as task


def _read_file(relative_path: str) -> str:
    """
    Lê o conteúdo de um arquivo, resolvendo o caminho absoluto
    relativo a este arquivo.
    """
    path = (BASE_DIR / relative_path).resolve()

    if not path.exists():
        msg = f"Arquivo HTML não encontrado: {path}"
        log(msg)
        raise FileNotFoundError(msg)

    with path.open("r", encoding="utf-8") as file_handle:
        return file_handle.read()


def _normalize_recipients(raw_recipients: Sequence[str]) -> List[str]:
    """
    Normaliza e valida a lista de destinatários.
    """

    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    recipients = []

    for recipient in raw_recipients:
        if not recipient:
            continue

        cleaned = recipient.strip()

        if not email_pattern.match(cleaned):
            log(f"E-mail inválido, ignorando: {cleaned}")
            continue

        recipients.append(cleaned)

    if not recipients:
        msg = "Lista de destinatários vazia após normalização."
        log(msg)
        raise ValueError(msg)

    return recipients


def _build_email_message(
    sender_email: str,
    sender_name: str,
    recipients: Sequence[str],
    subject: str,
    html_body_path: str,
    plain_body_path: str,
) -> EmailMessage:
    """
    Constrói a mensagem de e-mail base (sem anexos).
    """
    message = EmailMessage()

    message["From"] = f"{sender_name} <{sender_email}>"
    message["To"] = f"{sender_name} <{sender_email}>"
    message["Bcc"] = ", ".join(recipients)  # Usando Bcc para não expor destinatários
    message["Subject"] = subject

    try:
        plain_body = _read_file(plain_body_path)
        message.set_content(plain_body)
    except Exception as exc:
        log(f"Corpo em texto puro não fornecido ou falha ao ler: {exc}")

    try:
        html_body = _read_file(html_body_path)
        message.add_alternative(html_body, subtype="html")
    except Exception as exc:
        log(f"Corpo em HTML não fornecido ou falha ao ler: {exc}")

    return message


def _get_smtp_client_class(use_ssl: bool, smtp_port: int) -> Type[smtplib.SMTP]:
    """
    Retorna a classe de cliente SMTP apropriada (SMTP ou SMTP_SSL).
    """
    if use_ssl or smtp_port == 465:
        return smtplib.SMTP_SSL

    return smtplib.SMTP


def _send_prepared_message_via_smtp(
    message: EmailMessage,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    use_ssl: bool = False,
    timeout_seconds: int = 30,
) -> None:
    """
    Envia uma mensagem `EmailMessage` usando SMTP (STARTTLS por padrão).
    """
    smtp_class = _get_smtp_client_class(
        use_ssl=use_ssl,
        smtp_port=smtp_port,
    )

    try:
        # Conecta ao servidor SMTP
        with smtp_class(smtp_host, smtp_port, timeout=timeout_seconds) as server:
            # Se não estamos usando SSL direto, realiza o handshake STARTTLS
            if smtp_class is smtplib.SMTP:
                server.ehlo()
                server.starttls()
                server.ehlo()

            server.login(smtp_user, smtp_password)

            # Envia a mensagem já preparada
            server.send_message(message)

            log("E-mail enviado com sucesso.")

    except Exception as exc:
        log(f"Falha ao enviar e-mail via SMTP: {exc}")
        raise


@task
def make_email_meta_df(
    environment: str,
    subject: str,
    recipients: str,
    query_path: str,
    attachments: str,
    html_body_path: str,
    plain_body_path: str,
    sender_name: str,
    sender_email: str,
    smtp_user: str,
    smtp_host: str,
    smtp_port: str,
) -> pd.DataFrame:
    row = {
        "date": datetime.now(),
        "date_partition": datetime.now().strftime("%Y%m%d"),
        "environment": environment,
        "subject": subject,
        "recipients": recipients,
        "query_path": query_path,
        "attachments": attachments,
        "html_body_path": html_body_path,
        "plain_body_path": plain_body_path,
        "sender_name": sender_name,
        "sender_email": sender_email,
        "smtp_user": smtp_user,
        "smtp_host": smtp_host,
        "smtp_port": smtp_port,
    }

    df = pd.DataFrame([row])
    return df


@task
def delete_file_from_disk(filepath: str) -> None:
    """
    Apaga um arquivo do disco.
    """

    path = Path(filepath)
    if path.exists():
        path.unlink()
        log(f"Arquivo apagado do disco: {filepath}")
    else:
        log(f"Arquivo não encontrado para apagar: {filepath}")
