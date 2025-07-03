# -*- coding: utf-8 -*-
from pipelines.utils.credential_injector import (
    authenticated_task as task,  # importar aqui
)
from pipelines.utils.monitor import send_message


@task
def report_csv_validation_errors(blob_file: str, error_logs: list[str]):
    """
    Envia uma mensagem ao Discord com os erros encontrados na validação do CSV.

    Args:
        blob_file (str): Nome do arquivo (ex: posicao, pedidos...).
        error_logs (List[str]): Lista de erros encontrados na validação.
    """
    if not error_logs:
        return

    title = f"Erros de Validação no CSV - {blob_file.upper()}"

    max_lines = 15  # evitar mensagens muito longas no Discord
    trimmed_logs = error_logs[:max_lines]
    total = len(error_logs)

    message_lines = [
        f"⚠️ Foram encontrados {total} registros inválidos no arquivo `{blob_file}`.",
        f"Mostrando os primeiros {len(trimmed_logs)} erros:",
        "",
    ]
    message_lines += [f"🔴 {err}" for err in trimmed_logs]

    if total > max_lines:
        message_lines.append(f"... e mais {total - max_lines} erro(s).")

    full_message = "\n".join(message_lines)

    send_message(
        title=title,
        message=full_message,
        monitor_slug="data-ingestion",
    )
