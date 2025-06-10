# -*- coding: utf-8 -*-
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task
def create_and_send_final_report(operator_run_states: list):
    tables_not_found = {}
    db_not_found = []
    unexpected_failures = []

    for flow_run_state in operator_run_states:
        params = flow_run_state.context.get("parameters", {})
        cnes_code = params.get("CNES_CODE", "CNES_Desconhecido")

        if flow_run_state.is_failed():
            error_message = str(flow_run_state.result)
            if "Cannot open database" in error_message and "login failed" not in error_message:
                if cnes_code not in db_not_found:
                    db_not_found.append(cnes_code)
            else:
                unexpected_failures.append(
                    f"CNES {cnes_code}: Falha inesperada no flow! Erro: {error_message[:200]}..."
                )
            continue

        if flow_run_state.is_successful() and isinstance(flow_run_state.result, dict):
            task_states_dict = flow_run_state.result

            for task_run_state in task_states_dict.values():
                if task_run_state.is_skipped() and task_run_state.name.startswith(
                    "extract_and_transform_table"
                ):
                    message = task_run_state.message
                    if "Table not found:" in message:
                        table_name = message.split("'")[1]
                        if cnes_code not in tables_not_found:
                            tables_not_found[cnes_code] = []
                        tables_not_found[cnes_code].append(table_name)

                elif task_run_state.is_failed():
                    error_message = str(task_run_state.result)
                    unexpected_failures.append(
                        f"CNES {cnes_code}, Task '{task_run_state.name}': Falha inesperada! Erro: {error_message[:150]}..."
                    )

    total_runs = len(operator_run_states)

    all_problematic_cnes = set(db_not_found) | set(tables_not_found.keys())
    for failure in unexpected_failures:
        cnes_in_failure = failure.split(":")[0].replace("CNES", "").strip().split(",")[0]
        all_problematic_cnes.add(cnes_in_failure)

    successful_runs = total_runs - len(all_problematic_cnes)

    if not all_problematic_cnes:
        log("Todos os flows Backup Vitacare foram executados com sucesso!")
        send_message(
            title="Relatório Extração - Backup Vitacare",
            message=f"Execução finalizada com sucesso para todos os {total_runs} CNES.",
            monitor_slug="data-ingestion",
        )
        return

    title = "Relatório Extração - Backup Vitacare"
    message_lines = []

    if db_not_found:
        message_lines.append(f"**🔴 {len(db_not_found)} CNES com falha de acesso ao banco:**")
        message_lines.append("```")
        for cnes in sorted(db_not_found):
            message_lines.append(cnes)
        message_lines.append("```")
        message_lines.append("")

    if tables_not_found:
        message_lines.append(f"**🔴 {len(tables_not_found)} CNES com tabelas ausentes:**")
        message_lines.append("```")
        for cnes, tables in sorted(tables_not_found.items()):
            tables_str = ", ".join(sorted(list(set(tables))))
            message_lines.append(f"{cnes}: {tables_str}")
        message_lines.append("```")
        message_lines.append("")

    if unexpected_failures:
        message_lines.append("**🔴 FALHAS INESPERADAS:**")
        message_lines.append("```")
        for failure in unexpected_failures:
            message_lines.append(f"- {failure}")
        message_lines.append("```")

    if all_problematic_cnes:
        message_lines.append("\n-----------------------------------\n")
        message_lines.append(
            f"**Lista de CNES para Investigação ({len(all_problematic_cnes)} no total):**"
        )
        message_lines.append("```")
        for cnes in sorted(list(all_problematic_cnes)):
            message_lines.append(cnes)
        message_lines.append("```")

    message_lines.append(
        f"**Status Final:** {successful_runs}/{total_runs} CNES processados sem alertas."
    )

    final_message = "\n".join(message_lines)

    send_message(title=title, message=final_message, monitor_slug="data-ingestion")
