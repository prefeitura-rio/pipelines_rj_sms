# -*- coding: utf-8 -*-
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task
def create_and_send_final_report(operator_run_states: list):
    tables_not_found = {}
    db_not_found = []
    unexpected_failures = []

    for state in operator_run_states:
        params = state.context.get("parameters", {})
        cnes_code = params.get("CNES_CODE", "CNES_Desconhecido")

        if state.is_failed():
            error_message = str(state.result)

            if "Invalid object name" in error_message:
                table_name = error_message.split("'")[1].split(".")[-1]
                if cnes_code not in tables_not_found:
                    tables_not_found[cnes_code] = []
                tables_not_found[cnes_code].append(table_name)
            elif "Cannot open database" in error_message and "login failed" in error_message:
                if cnes_code not in db_not_found:
                    db_not_found.append(cnes_code)
            else:
                unexpected_failures.append(
                    f"CNES {cnes_code}: Falha inesperada! Erro: {error_message[:200]}..."
                )

    total_runs = len(operator_run_states)
    num_db_not_found = len(db_not_found)
    num_tables_not_found_cases = len(tables_not_found)
    successful_runs = total_runs - num_db_not_found - num_tables_not_found_cases

    if not tables_not_found and not db_not_found and not unexpected_failures:
        log("Todos os flows Backup Vitacare foram executados com sucesso!")
        return

    title = "Relatório Extração - Backup Vitacare"
    message_lines = []

    if db_not_found:
        message_lines.append(f"{num_db_not_found} backups não encontrados:")
        message_lines.append("")
        for cnes in sorted(db_not_found):
            message_lines.append(cnes)
        message_lines.append("")

    if tables_not_found:
        message_lines.append(f"{num_tables_not_found_cases} casos de tabelas ausentes:")
        message_lines.append("")
        for cnes, tables in sorted(tables_not_found.items()):
            tables_str = ", ".join(sorted(list(set(tables))))
            message_lines.append(f"{cnes}: {tables_str}")
        message_lines.append("")

    message_lines.append(f"Status Final: {successful_runs}/{total_runs}")

    if unexpected_failures:
        message_lines.append("\n-----------------------------------\n")
        message_lines.append("FALHAS INESPERADAS:")
        for failure in unexpected_failures:
            message_lines.append(f"- {failure}")

    final_message = "\n".join(message_lines)

    send_message(title=title, message=final_message, monitor_slug="data-ingestion")
