# -*- coding: utf-8 -*-
import mimetypes
import unicodedata
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Optional, Sequence, Tuple

import pandas as pd
from google.cloud import bigquery
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.reports.emails_subgeral_gestao.constants import BASE_DIR
from pipelines.reports.utils.emails_subgeral import (
    _build_email_message,
    _normalize_recipients,
    _read_file,
    _send_prepared_message_via_smtp,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import create_folders, download_from_url


def _extract_emails_from_csv(folder: dict, gsheets_sheet_name: str) -> Sequence[str]:
    """
    Lê o CSV em `csv_path` e retorna uma lista com um email por linha
    da coluna chamada "email". Retorna uma lista vazia se a coluna
    não existir. Remove valores NA e remove espaços em branco.
    """
    csv_file = Path(folder["raw"]) / f"{gsheets_sheet_name}.csv"

    df = pd.read_csv(csv_file, delimiter="|")
    if "email" not in df.columns:
        return []

    emails = df["email"].dropna().astype(str).str.strip().tolist()
    return emails


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_recipients_from_gsheets(gsheets_url: str, gsheets_sheet_name: str) -> Sequence[str]:
    """
    Extrai lista de emails de uma planilha Google Sheets e escreve o arquivo em disco.
    """
    folder = create_folders.run()

    download_from_url.run(
        url_type="google_sheet",
        url=gsheets_url,
        gsheets_sheet_name=gsheets_sheet_name,
        file_name=gsheets_sheet_name,
        file_path=folder["raw"],
        csv_delimiter="|",
    )

    recipients = _extract_emails_from_csv(folder, gsheets_sheet_name)

    return recipients


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def bigquery_to_xl_disk(subject: str, query_path: str) -> Optional[str]:
    """
    Executa uma consulta no BigQuery, salva em Excel e retorna o caminho absoluto do arquivo.
    """

    if not query_path or not subject:
        log("Erro: query_path ou subject não fornecidos")
        return None

    query = _read_file(query_path)

    client = bigquery.Client()
    log("Cliente Big Query OK")

    df = client.query_and_wait(query).to_dataframe()
    log("Query executada com sucesso")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_subject = unicodedata.normalize("NFKD", subject).encode("ascii", "ignore").decode("ascii")
    safe_subject = safe_subject.replace(" ", "_")
    filename = f"{safe_subject}__{timestamp}.xlsx"

    folder = create_folders.run()
    out_dir = Path(folder["raw"]) / "emails_subgeral_gestao"
    out_dir.mkdir(parents=True, exist_ok=True)

    filepath = (out_dir / filename).resolve()

    df.to_excel(filepath, index=False)
    log(f"Arquivo salvo: {filepath}")

    return str(filepath)


def _guess_mime_type(path: Path) -> Tuple[str, str]:
    """
    Descobre o tipo MIME de um arquivo.
    """
    ctype, _ = mimetypes.guess_type(path)
    maintype, subtype = (ctype or "application/octet-stream").split("/", 1)

    return maintype, subtype


def _add_attachments_to_message(
    message: EmailMessage,
    attachments: str,
) -> None:
    """
    Adiciona anexos ao objeto `EmailMessage`.
    """
    if not attachments:
        return

    raw_path = Path(attachments)

    # Se for relativo, resolve em relação ao BASE_DIR
    if not raw_path.is_absolute():
        path = (BASE_DIR / raw_path).resolve()
    else:
        path = raw_path

    # Se o arquivo não existir, registra aviso e segue para o próximo.
    if not path.exists():
        log(f"Anexo não encontrado, ignorando: {path}")
        return

    maintype, subtype = _guess_mime_type(path)

    # Lê o conteúdo binário do arquivo
    with path.open("rb") as file_handle:
        file_bytes = file_handle.read()

    # Adiciona o anexo ao e-mail
    message.add_attachment(
        file_bytes,
        maintype=maintype,
        subtype=subtype,
        filename=path.name,
    )


# tarefa principal que orquestra os passos para enviar email via smtp
@task(max_retries=2, retry_delay=timedelta(minutes=2))
def send_email_smtp(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    sender_email: str,
    sender_name: str,
    recipients: Sequence[str],
    subject: str,
    html_body_path: str,
    plain_body_path: Optional[str] = None,
    attachments: Optional[str] = None,
    use_ssl: bool = False,
) -> None:
    """
    Função de alto nível para envio de e-mails via SMTP.
    """

    # Normaliza e valida a lista de destinatários
    normalized_recipients = _normalize_recipients(recipients)

    # Constrói a mensagem (sem anexos / imagens)
    message = _build_email_message(
        sender_email=sender_email,
        sender_name=sender_name,
        recipients=normalized_recipients,
        subject=subject,
        html_body_path=html_body_path,
        plain_body_path=plain_body_path,
    )

    if attachments:
        # Adiciona anexos, se houver
        _add_attachments_to_message(
            message=message,
            attachments=attachments,
        )

    # Envia a mensagem
    _send_prepared_message_via_smtp(
        message=message,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        use_ssl=use_ssl,
    )
