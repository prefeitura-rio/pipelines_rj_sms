# -*- coding: utf-8 -*-
import re
from collections import defaultdict

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message


@task
def create_and_send_final_report(operator_run_states: list):
    """
    Analisa os resultados dos fluxos de tabela e envia um relatório final consolidado
    """
    TASK_NAME_TO_CHECK = "process_cnes_table"

    report = {}
    total_tables = len(operator_run_states)
    successful_tables = 0

    def get_clean_reason(state):
        if isinstance(state.result, dict) and state.result.get("reason"):
            return state.result["reason"]

        message = str(state.message).lower()
        if "database" in message and "does not exist" in message:
            return "Database não encontrado"
        if "table not found" in message or "invalid object name" in message:
            return "Tabela não encontrada"
        if "no data extracted" in message:
            return "Nenhum dado extraído"

        if state.is_failed() and state.message:
            return str(state.message).split("\n")[0][:100]

        return "Motivo desconhecido"

    for flow_run in operator_run_states:
        params = flow_run.parameters or {}
        table_name = params.get("TABLE_NAME", "Tabela_Desconhecida")

        report[table_name] = {
            "failed_cnes": defaultdict(list),
            "skipped_cnes": defaultdict(list),
            "flow_error": None,
        }

        flow_state = flow_run.state

        if flow_state.is_failed():
            report[table_name]["flow_error"] = str(flow_state.result)[:300]
            continue

        if flow_state.is_successful() and isinstance(flow_state.result, dict):
            task_states_dict = flow_state.result
            has_issues = False

            for task_run_state in task_states_dict.values():
                if not task_run_state.name.startswith(TASK_NAME_TO_CHECK):
                    continue

                task_result = task_run_state.result or {}
                cnes_code = task_result.get("cnes", "CNES_Desconhecido")
                reason = get_clean_reason(task_run_state)

                if task_run_state.is_failed():
                    report[table_name]["failed_cnes"][reason].append(cnes_code)
                    has_issues = True
                elif task_run_state.is_skipped():
                    report[table_name]["skipped_cnes"][reason].append(cnes_code)
                    has_issues = True

            if not has_issues and not report[table_name]["flow_error"]:
                successful_tables += 1

    # Construção da mensagem final
    if successful_tables == total_tables:
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
            for reason, cnes_list in results["failed_cnes"].items():
                cnes_str = ", ".join(sorted(cnes_list))
                message_lines.append(f"  - 🔴 Falha (`{reason}`): CNES `{cnes_str}`")

        if results["skipped_cnes"]:
            for reason, cnes_list in results["skipped_cnes"].items():
                cnes_str = ", ".join(sorted(cnes_list))
                message_lines.append(f"  - ⚠️ Skip (`{reason}`): CNES `{cnes_str}`")

        message_lines.append("")

    message_lines.append("\n-----------------------------------\n")
    message_lines.append(
        f"**Status Final:** {successful_tables}/{total_tables} tabelas processadas sem alertas."
    )
    final_message = "\n".join(message_lines)
    send_message(title=title, message=final_message, monitor_slug="data-ingestion")
