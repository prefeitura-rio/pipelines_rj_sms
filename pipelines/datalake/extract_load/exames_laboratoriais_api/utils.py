from prefeitura_rio.pipelines_utils.logging import log
from pipelines.utils.monitor import send_email

def send_api_error_report(status_code, source, environment, api_response):
    """
    Envia um e-mail simples notificando indisponibilidade da API (502/503)
    """
    subject = f"🚨 {source} API Indisponível"

    api_response = api_response[:300] + "..." if len(api_response) > 500 else api_response
    
    message = f"""
    <h2>Erro de Conectividade na API</h2>
    <p>A extração de exames laboratoriais encontrou um erro de servidor.</p>
    <ul>
        <li><b>Sistema:</b> {source}</li>
        <li><b>Status Code:</b> {status_code}</li>
        <li><b>Ambiente:</b> {environment}</li>
    </ul>

    <h3>Resposta do Servidor:</h3>
    <div 
        <pre{api_response}</pre>
    </div>
    
    <p>O fluxo continuará tentando conforme a política de retries, mas este e-mail serve para avisar sobre a instabilidade no fornecedor.</p>
    """

    recipients = ["daniel.lira@dados.rio"]

    try:
        send_email(
            subject=subject,
            message=message,
            recipients={
                "to_addresses": recipients,
                "cc_addresses": [],
                "bcc_addresses": [],
            },
        )
        log(f"E-mail de alerta enviado para {recipients}")
    except Exception as e:
        log(f"Falha ao enviar e-mail de alerta: {str(e)}", level="error")