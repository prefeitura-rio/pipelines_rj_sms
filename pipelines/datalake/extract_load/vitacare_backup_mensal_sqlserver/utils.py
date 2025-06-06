# -*- coding: utf-8 -*-
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task
def create_and_send_final_report(operator_run_states: list):
    """
    Analisa os estados finais dos flow runs dos Operators, consolida os erros
    esperados e envia um único relatório.
    """
    expected_failures = {}  # Dicionário para guardar erros esperados
    non_existent_backups = []  # Para os Cnes sem backup
    unexpected_failures = []  # Lista para guardar erros graves/inesperados

    for state in operator_run_states:
        # Pega o código CNES dos parâmetros daquele flow run
        params = state.context.get("parameters", {})
        cnes_code = params.get("CNES_CODE", "CNES_Desconhecido")

        if state.is_failed():
            error_message = str(state.result)

            if "Invalid object name" in error_message:
                table_name = error_message.split("'")[1].split(".")[-1]
                if cnes_code not in expected_failures:
                    expected_failures[cnes_code] = []
                expected_failures[cnes_code].append(table_name)

            elif "Cannot open database" in error_message and "login failed" in error_message:
                if cnes_code not in non_existent_backups:
                    non_existent_backups.append(cnes_code)

            else:
                unexpected_failures.append(
                    f"CNES {cnes_code}: Falha inesperada! Erro: {error_message[:200]}..."
                )

    if not expected_failures and not unexpected_failures and not non_existent_backups:
        log("Todos os flows Backup Vitacare foram executados com sucesso!")
        return

    # Montaa a mensagem final
    title = "📄 Relatório Final de Extração - Backup Vitacare"
    message_lines = []

    if non_existent_backups:
        message_lines.append("⚠️ **CNES Sem Backup de Dados:**")
        for cnes in sorted(non_existent_backups):
            message_lines.append(f"- O banco de dados para o CNES `{cnes}` não foi encontrado.")
        message_lines.append("\n-----------------------------------\n")

    if expected_failures:
        message_lines.append("**Tabelas Não Encontradas:**")
        for cnes, tables in sorted(expected_failures.items()):
            message_lines.append(f"\n**CNES: `{cnes}`**")
            for table in sorted(list(set(tables))):
                message_lines.append(f"- Tabela `{table}` não encontrada")

    if unexpected_failures:
        message_lines.append("\n-----------------------------------\n")
        message_lines.append("🔴 **FALHAS INESPERADAS!**")
        for failure in unexpected_failures:
            message_lines.append(f"- {failure}")

    final_message = "\n".join(message_lines)

    send_message(title=title, message=final_message, monitor_slug="ingestion")
    log("Relatório consolidado de extração enviado.")
