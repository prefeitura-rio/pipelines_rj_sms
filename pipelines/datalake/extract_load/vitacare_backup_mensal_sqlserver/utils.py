# -*- coding: utf-8 -*-
import re
from collections import defaultdict
from datetime import datetime
import pytz
import pandas as pd
from pipelines.utils.data_cleaning import remove_columns_accents

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


# --- Funções auxiliares para pré-processamento ---

def clean_ut_id(val):
    """
    Decodifica e limpa valores VARBINARY de 'ut_id'.
    Versão aprimorada com tratamento para caracteres de controle e representação hexadecimal.
    """
    if isinstance(val, bytes):
        try:
            decoded_val = val.decode("utf-16-le", errors="ignore")
        except UnicodeDecodeError:
            decoded_val = val.decode("latin-1", errors="ignore")

        # Remove caracteres nulos e normaliza quebras de linha/tabulações
        clean_str = (
            decoded_val.replace("\x00", "")
            .replace("\n", " ")
            .replace("\r", " ")
            .replace("\t", " ")
        )
        # Verifica se ainda há caracteres de controle não-espaço após a limpeza básica
        # Se houver, retorna a representação hexadecimal para garantir uma string plana
        if any(ord(c) < 32 and c not in (" ",) for c in clean_str):
            return val.hex() # Retorna a representação hexadecimal dos bytes originais
        else:
            return clean_str.strip()
    elif isinstance(val, str):
        # Trata strings que parecem representações de bytes (e.g., b'...')
        if val.startswith("b'") and val.endswith("'"):
            try:
                # Tenta converter a representação de string de bytes para bytes reais
                byte_repr = (
                    val[2:-1].encode("latin-1").decode("unicode_escape").encode("latin-1")
                )
                try:
                    decoded_val = byte_repr.decode("utf-16-le", errors="ignore")
                except UnicodeDecodeError:
                    decoded_val = byte_repr.decode("latin-1", errors="ignore")

                clean_str = (
                    decoded_val.replace("\x00", "")
                    .replace("\n", " ")
                    .replace("\r", " ")
                    .replace("\t", " ")
                )
                if any(ord(c) < 32 and c not in (" ",) for c in clean_str):
                    return byte_repr.hex() # Retorna a representação hexadecimal dos bytes reais
                else:
                    return clean_str.strip()
            except Exception:
                # Em caso de erro na conversão de string de bytes, fallback para tratamento de string simples
                pass

        # Para strings que não são representações de bytes ou falham na conversão de bytes
        return (
            val.replace("\x00", "")
            .replace("\n", " ")
            .replace("\r", " ")
            .replace("\t", " ")
            .strip()
        )
    # Para qualquer outro tipo, converte para string e limpa
    return (
        str(val)
        .replace("\x00", "")
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
        .strip()
    )

# Definir as colunas que precisam do tratamento especial de aspas duplas
SPECIAL_TEXT_COLUMNS_FOR_QUOTING = {
    "subjetivo_motivo",
    "plano_observacoes",
    "avaliacao_observacoes",
    "objetivo_descricao",
    "notas_observacoes",
}


def transform_dataframe(df: pd.DataFrame, cnes_code: str, db_table: str) -> pd.DataFrame:
    """
    Aplica transformações aos DataFrames extraídos da Vitacare.
    """
    if df.empty:
        return df

    now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
    df["extracted_at"] = now
    df["id_cnes"] = cnes_code

    df.columns = remove_columns_accents(df)

    # Aplica clean_ut_id se a coluna existe
    if "ut_id" in df.columns:
        df["ut_id"] = df["ut_id"].apply(clean_ut_id)

    # Processa colunas de texto (do tipo object no Pandas)
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str) # Garante que a coluna é string para operações de str

        # Limpeza básica de caracteres especiais
        df[col] = df[col].str.replace(r"[\n\r\t\x00]+", " ", regex=True)

        # APLICA TRATAMENTO ESPECIAL DE ASPAS DUPLAS SOMENTE PARA COLUNAS ESPECÍFICAS
        # E SOMENTE PARA A TABELA 'ATENDIMENTOS'
        if db_table.upper() == "ATENDIMENTOS" and col.lower() in SPECIAL_TEXT_COLUMNS_FOR_QUOTING:
            # Primeiro, escapa as aspas duplas internas (duas aspas para uma)
            df[col] = df[col].str.replace('"', '""', regex=False)
            # Segundo, envolve toda a string em aspas duplas
            df[col] = '"' + df[col] + '"'


    # Trata a coluna 'acto_id' como numérico se presente
    if "acto_id" in df.columns:
        # Tenta converter para numérico, coercing erros para NaN
        df["acto_id"] = pd.to_numeric(df["acto_id"], errors="coerce")
        # Converte para Int64Dtype (inteiro com suporte a valores nulos)
        df["acto_id"] = df["acto_id"].astype(pd.Int64Dtype())

    return df