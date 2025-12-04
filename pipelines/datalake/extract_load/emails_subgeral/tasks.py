import logging
import mimetypes
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Type

# Configuração básica de logging do módulo
logger = logging.getLogger(__name__)


# Tipos auxiliares
AttachmentPaths = Optional[Sequence[str]]
InlineImages = Optional[Dict[str, str]]


# Funções utilitárias relacionadas a MIME / arquivos
def guess_mime_type(path: Path) -> Tuple[str, str]:
    """
    Descobre o tipo MIME de um arquivo.
    """
    ctype, _ = mimetypes.guess_type(path)
    maintype, subtype = (ctype or "application/octet-stream").split("/", 1)

    return maintype, subtype


# Funções de preparação / validação dos dados do e-mail
def normalize_recipients(raw_recipients: Sequence[str]) -> List[str]:
    """
    Normaliza e valida a lista de destinatários.
    """
    recipients = [
        recipient.strip() for recipient in raw_recipients if recipient and recipient.strip()
    ]

    if not recipients:
        msg = "Lista de destinatários vazia após normalização."
        logger.error(msg)
        raise ValueError(msg)

    return recipients


def build_email_message(
    sender_email: str,
    sender_name: str,
    recipients: Sequence[str],
    subject: str,
    html_body: str,
    plain_body: Optional[str] = None,
) -> EmailMessage:
    """
    Constrói a mensagem de e-mail base (sem anexos e sem imagens inline).
    """
    message = EmailMessage()

    message["From"] = f"{sender_name} <{sender_email}>"
    message["To"] = f"{sender_name} <{sender_email}>"
    message["Bcc"] = ", ".join(recipients) # Usando Bcc para não expor destinatários
    message["Subject"] = subject

    # Conteúdo em texto puro (para clientes que não renderizam HTML)
    plain_text_body = plain_body or "Mensagem sem versão em texto simples."
    message.set_content(plain_text_body)

    # Adiciona a versão em HTML
    message.add_alternative(html_body, subtype="html")

    return message


# Funções para anexar arquivos e imagens inline
def add_attachments_to_message(
    message: EmailMessage,
    attachments: AttachmentPaths,
) -> None:
    """
    Adiciona anexos ao objeto `EmailMessage`.
    """
    if not attachments:
        return

    for attach_path in attachments:
        path = Path(attach_path)

        # Se o arquivo não existir, registra aviso e segue para o próximo.
        if not path.exists():
            logger.warning("Anexo não encontrado, ignorando: %s", attach_path)
            continue

        maintype, subtype = guess_mime_type(path)

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


def add_inline_images_to_message(
    message: EmailMessage,
    inline_images: InlineImages,
) -> None:
    """
    Adiciona imagens inline (referenciadas por Content-ID) ao e-mail.
    """
    if not inline_images:
        return

    for content_id_name, img_path in inline_images.items():
        path = Path(img_path)

        # Se a imagem não existir, registra aviso e segue.
        if not path.exists():
            logger.warning("Imagem inline não encontrada, ignorando: %s", img_path)
            continue

        maintype, subtype = guess_mime_type(path)

        # Lê a imagem em bytes
        with path.open("rb") as file_handle:
            image_bytes = file_handle.read()

        # Adiciona inicialmente como anexo comum
        message.add_attachment(
            image_bytes,
            maintype=maintype,
            subtype=subtype,
            filename=path.name,
        )

        # Recupera o payload para acessar a última parte adicionada
        payload = message.get_payload()

        # Garante que o payload é uma lista de partes (com alternativa HTML, anexos etc.)
        if isinstance(payload, list) and payload:
            last_part: EmailMessage = payload[-1]

            # Define o Content-ID que será referenciado no HTML
            last_part.add_header("Content-ID", f"<{content_id_name}>")

            # Marca o conteúdo como inline para aparecer embutido no corpo
            last_part.add_header(
                "Content-Disposition",
                "inline",
                filename=path.name,
            )


# Funções de envio via SMTP
def get_smtp_client_class(use_ssl: bool, smtp_port: int) -> Type[smtplib.SMTP]:
    """
    Retorna a classe de cliente SMTP apropriada (SMTP ou SMTP_SSL).
    """
    if use_ssl or smtp_port == 465:
        return smtplib.SMTP_SSL

    return smtplib.SMTP


def send_prepared_message_via_smtp(
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
    smtp_class = get_smtp_client_class(
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

        logger.info("E-mail enviado com sucesso para: %s", message["To"])

    except Exception as exc:
        logger.exception("Falha ao enviar e-mail via SMTP: %s", exc)
        raise


# Função que orquestra todo o envio
def send_email_smtp(
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    sender_email: str,
    sender_name: str,
    recipients: Sequence[str],
    subject: str,
    html_body: str,
    plain_body: Optional[str] = None,
    attachments: AttachmentPaths = None,
    inline_images: InlineImages = None,
    use_ssl: bool = False,
) -> None:
    """
    Função de alto nível para envio de e-mails via SMTP.
    """
    # Normaliza e valida a lista de destinatários
    normalized_recipients = normalize_recipients(recipients)

    # Constrói a mensagem (sem anexos / imagens)
    message: EmailMessage = build_email_message(
        sender_email=sender_email,
        sender_name=sender_name,
        recipients=normalized_recipients,
        subject=subject,
        html_body=html_body,
        plain_body=plain_body,
    )

    # Adiciona anexos, se houver
    add_attachments_to_message(
        message=message,
        attachments=attachments,
    )

    # Adiciona imagens inline, se houver
    add_inline_images_to_message(
        message=message,
        inline_images=inline_images,
    )

    # Envia a mensagem
    send_prepared_message_via_smtp(
        message=message,
        smtp_host=smtp_host,
        smtp_port=smtp_port,
        smtp_user=smtp_user,
        smtp_password=smtp_password,
        use_ssl=use_ssl,
    )
