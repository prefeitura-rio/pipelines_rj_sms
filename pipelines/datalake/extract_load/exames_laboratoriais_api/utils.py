# -*- coding: utf-8 -*-
from markdown_it import MarkdownIt
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.utils.monitor import send_email

def send_api_error_report(status_code, source, environment):
    
    md = MarkdownIt()
    
    subject = f"🚨 [ALERTA] API {source.upper()} Indisponível"

    # Construção da mensagem em Markdown
    message = f"## A extração de exames laboratoriais encontrou um erro de servidor.`\n\n"
    message += f"- **Sistema:** {source.upper()}\n"
    message += f"- **Status Code:** {status_code}\n"
    message += f"- **Ambiente:** {environment}\n"
    message += f"- **Motivo:** Servidor em manutenção ou indisponível.\n\n"
    message += "---\n"
    message += "*O fluxo continuará tentando a extração automaticamente conforme a política de retries.*"

    recipients = ["daniel.lira@prefeitura.rio"]

    try:
        send_email(
            subject=subject,
            message=md.render(message),  
            recipients={
                "to_addresses": recipients,
                "cc_addresses": [],
                "bcc_addresses": [],
            },
        )
        log(f"E-mail de alerta enviado com sucesso para {recipients}")
    except Exception as e:
        log(f"Falha ao enviar e-mail de alerta: {str(e)}", level="error")