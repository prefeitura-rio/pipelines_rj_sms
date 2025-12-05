import pandas as pd
from google.cloud import bigquery
from datetime import timedelta
import mimetypes
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import List, Optional, Sequence, Tuple, Type

from prefeitura_rio.pipelines_utils.logging import log
from pipelines.utils.credential_injector import authenticated_task as task
from datetime import datetime


BASE_DIR = Path(__file__).resolve().parent


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


# Tarefa principal para extrair dados do BigQuery
@task(max_retries=3, retry_delay=timedelta(minutes=5))
def bigquery_to_xl_disk(subject: str, query_path: str) -> Optional[str]:
    """
    Executa uma consulta no BigQuery, salva em Excel e retorna o caminho absoluto do arquivo.
    """

    if not query_path:
        return None

    query = _read_file(query_path)

    client = bigquery.Client()
    log("Cliente Big Query OK")

    df = client.query_and_wait(query).to_dataframe()
    log(query)
    log("Query executada com sucesso")
    log(f"{df.sample(5)}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    safe_subject = subject.replace(" ", "_")
    filename = f"{safe_subject}__{timestamp}.xlsx"

    # caminho baseado no arquivo atual, não no cwd
    filepath = (BASE_DIR / filename).resolve()

    df.to_excel(filepath, index=False)
    log(f"Arquivo salvo: {filepath}")

    return str(filepath)


# Apaga arquivo do disco
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


# Funções utilitárias relacionadas a MIME / arquivos
def _guess_mime_type(path: Path) -> Tuple[str, str]:
    """
    Descobre o tipo MIME de um arquivo.
    """
    ctype, _ = mimetypes.guess_type(path)
    maintype, subtype = (ctype or "application/octet-stream").split("/", 1)

    return maintype, subtype


# Funções de preparação / validação dos dados do e-mail
def _normalize_recipients(raw_recipients: Sequence[str]) -> List[str]:
    """
    Normaliza e valida a lista de destinatários.
    """
    recipients = [
        recipient.strip() for recipient in raw_recipients if recipient and recipient.strip()
    ]

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


# Funções para anexar arquivos
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

    # Se for relativo, resolvemos em relação ao BASE_DIR
    if not raw_path.is_absolute():
        path = (BASE_DIR / raw_path).resolve()
    else:
        path = raw_path

    # Se o arquivo não existir, registra aviso e segue para o próximo.
    if not path.exists():
        log(f"Anexo não encontrado, ignorando: {path}")

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


# Funções de envio via SMTP
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


# Função que orquestra todo o envio
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
    attachments: Optional[Sequence[str]] = None,
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


# Função que cria um dataframe com metadados para log
@task
def make_meta_df(
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
