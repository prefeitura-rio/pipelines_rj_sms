# -*- coding: utf-8 -*-
import re

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task
def create_and_send_final_report(operator_run_states: list):
    """
    Analisa os resultados dos fluxos de tabela e envia um relatório final consolidado.
    """
    report = {}
    total_tables = len(operator_run_states)
    successful_tables = 0

    def get_cnes_from_message(message: str) -> str:
        # Tenta extrair CNES de mensagens como '... for CNES: 1234567' ou '... vitacare_historic_1234567'
        match = re.search(r"(?:CNES: |vitacare_historic_)(\d{7})", message)
        if match:
            return match.group(1)
        return "CNES_Desconhecido"

    for flow_run_state in operator_run_states:
        params = flow_run_state.context.get("parameters", {})
        table_name = params.get("TABLE_NAME", "Tabela_Desconhecida")
        report[table_name] = {"failed_cnes": set(), "skipped_cnes": set(), "flow_error": None}

        if flow_run_state.is_failed():
            report[table_name]["flow_error"] = str(flow_run_state.result)[:300]
            continue

        if flow_run_state.is_successful() and isinstance(flow_run_state.result, dict):
            task_states_dict = flow_run_state.result
            has_issues = False

            for task_run_state in task_states_dict.values():
                if not task_run_state.name.startswith("extract_and_transform_table"):
                    continue

                message = str(task_run_state.message) or str(task_run_state.result)
                cnes_code = get_cnes_from_message(message)

                if task_run_state.is_failed():
                    report[table_name]["failed_cnes"].add(cnes_code)
                    has_issues = True
                elif task_run_state.is_skipped():
                    report[table_name]["skipped_cnes"].add(cnes_code)
                    has_issues = True

            if not has_issues:
                successful_tables += 1

    # Construção da mensagem final
    if successful_tables == total_tables:
        log("Todos os flows de extração por tabela foram executados com sucesso!")
        send_message(
            title="Relatório Extração - Backup Vitacare",
            message=f"Execução finalizada com sucesso para todas as {total_tables} tabelas.",
            monitor_slug="data-ingestion",
        )
        return

    title = "Relatório Extração - Backup Vitacare"
    message_lines = []

    for table, results in sorted(report.items()):
        if not results["failed_cnes"] and not results["skipped_cnes"] and not results["flow_error"]:
            continue

        message_lines.append(f"**Tabela: {table}**")
        if results["flow_error"]:
            message_lines.append(f"  - 🔴 Falha geral no fluxo: ```{results['flow_error']}...```")
        if results["failed_cnes"]:
            cnes_list = ", ".join(sorted(list(results["failed_cnes"])))
            message_lines.append(f"  - 🔴 Falha na extração para os CNES: `{cnes_list}`")
        if results["skipped_cnes"]:
            cnes_list = ", ".join(sorted(list(results["skipped_cnes"])))
            message_lines.append(f"  - ⚠️ Tabelas não encontradas para os CNES: `{cnes_list}`")
        message_lines.append("")

    message_lines.append("\n-----------------------------------\n")
    message_lines.append(
        f"**Status Final:** {successful_tables}/{total_tables} tabelas processadas sem alertas."
    )

    final_message = "\n".join(message_lines)
    send_message(title=title, message=final_message, monitor_slug="data-ingestion")