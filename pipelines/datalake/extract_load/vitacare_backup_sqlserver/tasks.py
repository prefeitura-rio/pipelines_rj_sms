# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""
from datetime import timedelta

import pandas as pd
from google.cloud import bigquery
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError 
from collections import defaultdict

from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.utils import (
    transform_dataframe,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.monitor import send_message
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(minutes=2))
def process_cnes_table(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_schema: str,
    db_table: str,
    cnes_code: str,
    dataset_id: str,
    partition_column: str,
) -> dict:

    bq_table_rename = {
        "ATENDIMENTOS": "acto",
        "PACIENTES": "cadastro",
    }
    bq_table_id = bq_table_rename.get(db_table.upper(), db_table.lower())

    full_table_name = f"{db_schema}.{db_table}"
    log(f"[process_cnes_table] Iniciando extração para tabela {db_table} do CNES: {cnes_code}")
    
    db_name = f"vitacare_historic_{cnes_code}"

    connection_string = (
        f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        "?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    )

    try:
        engine = create_engine(connection_string)

    #  Captura erro de conexão ou banco de dados inexistente
    except OperationalError as e: 
        log(f"[process_cnes_table] Erro de conexão/banco para CNES {cnes_code}: {e}", level="error")
        return { 
            "table": db_table,
            "cnes": cnes_code,
            "status": "ERRO_BANCO_NAO_ENCONTRADO",
            "message": str(e)
        }
    except Exception as e: 
        log(f"[process_cnes_table] Erro inesperado ao conectar para CNES {cnes_code}: {e}", level="error")
        return { 
            "table": db_table,
            "cnes": cnes_code,
            "status": "ERRO_CONEXAO_INESPERADO",
            "message": str(e)
        }

    query = f"SELECT * FROM {full_table_name}"

    total_rows = 0
    is_first_chunk = True

    try:
        chunks = pd.read_sql(query, engine, chunksize=500000)

    # Captura erro de tabela inexistente
    except ProgrammingError as e: 
        if "Invalid object name" in str(e) or "does not exist" in str(e): 
            log(f"[process_cnes_table] Tabela '{db_table}' não encontrada no CNES {cnes_code}. Erro: {e}", level="warning")
            return { 
                "table": db_table,
                "cnes": cnes_code,
                "status": "ERRO_TABELA_NAO_ENCONTRADA",
                "message": str(e)
            }
        else: 
            log(f"[process_cnes_table] Erro de SQL inesperado na tabela '{db_table}' do CNES {cnes_code}: {e}", level="error")
            return { # 
                "table": db_table,
                "cnes": cnes_code,
                "status": "ERRO_SQL_INESPERADO",
                "message": str(e)
            }
    
    # Flag para verificar se algum dado foi processado mesmo que venha vazio em chunks
    data_processed_successfully = False 

    for chunk in chunks:
        dataframe = transform_dataframe(chunk, cnes_code, db_table)

        if dataframe.empty:
            log(
                f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} vazia. Upload ignorado",
                level="warning"
            )
            continue

        upload_df_to_datalake.run(
            df=dataframe,
            dataset_id=dataset_id,
            table_id=bq_table_id,
            partition_column=partition_column,
            source_format="parquet",
            if_exists="replace" if is_first_chunk else "append",
            if_storage_data_exists="replace" if is_first_chunk else "append",
        )
        
        is_first_chunk = False
        total_rows += len(dataframe)
        data_processed_successfully = True
        
        log(
            f"[process_cnes_table] Upload feito para o CNES {cnes_code} "
            f"Total de linhas: {total_rows}"
        )

    if not data_processed_successfully and total_rows == 0:
        log(f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} está vazia", level="warning")
        return {
            "table": db_table,
            "cnes": cnes_code,
            "status": "VAZIA",
            "message": "Nenhum dado extraído" 
        }
    else:
        log(f"[process_cnes_table] Tabela '{db_table}' do CNES {cnes_code} processada com sucesso.", level="info")
        return {
            "table": db_table,
            "cnes": cnes_code,
            "status": "OK",
            "message": f"Total de linhas: {total_rows}" 
        }
        

@task(max_retries=2, retry_delay=timedelta(minutes=1))
def get_vitacare_cnes_from_bigquery() -> list:
    query = """
        SELECT DISTINCT id_cnes
        FROM `rj-sms.saude_dados_mestres.estabelecimento`
        WHERE prontuario_versao = 'vitacare'
        AND prontuario_episodio_tem_dado = 'sim'
    """
    log(f"Buscando códigos CNES do BigQuery com a query: {query}")
    try:
        client = bigquery.Client()
        query_job = client.query(query)
        cnes_list = [str(row.id_cnes) for row in query_job if row.id_cnes is not None]
        if not cnes_list:
            log("[get_vitacare_cnes_from_bigquery] Nenhum código CNES encontrado no BigQuery", level="warning")
            return []
        log(f"[get_vitacare_cnes_from_bigquery] Encontrados {len(cnes_list)} unidades no BigQuery.")
        return cnes_list
    except Exception as e:
        log(f"[get_vitacare_cnes_from_bigquery] Erro ao buscar códigos CNES do BigQuery: {e}", level="error")
        raise


@task
def get_tables_to_extract() -> list:
    return vitacare_constants.TABLES_TO_EXTRACT.value

@task
def build_operator_params(tables: list, env: str, schema: str, part_col: str) -> list:
    params_list = []
    for table in tables:
        params_list.append(
            {
                "TABLE_NAME": table,
                "environment": env,
                "DB_SCHEMA": schema,
                "PARTITION_COLUMN": part_col,
                "RENAME_FLOW": True,
            }
        )
    return params_list

@task
def consolidate_cnes_table_results(cnes_table_statuses: list) -> dict:
    """
    Consolida os resultados de processamento de todos os CNES para uma única tabela
    agrupando os CNES por status
    """
    consolidated_results = {
        "ok_cnes": [],
        "banco_nao_encontrado_cnes": [],
        "tabela_nao_encontrada_cnes": [],
        "vazia_cnes": [],
        "inesperado_cnes": [], # 
    }

    if cnes_table_statuses:
        table_name = cnes_table_statuses[0].get("table", "DESCONHECIDA")

    for result in cnes_table_statuses:
        status = result.get("status")
        cnes = result.get("cnes")
        message = result.get("message") 

        if status == "OK":
            consolidated_results["ok_cnes"].append(cnes)
        elif status == "VAZIA":
            consolidated_results["vazia_cnes"].append(cnes)
        elif status == "ERRO_BANCO_NAO_ENCONTRADO":
            consolidated_results["banco_nao_encontrado_cnes"].append(cnes)
        elif status == "ERRO_TABELA_NAO_ENCONTRADA":
            consolidated_results["tabela_nao_encontrada_cnes"].append(cnes)
        else:
            consolidated_results["inesperado_cnes"].append({"cnes": cnes, "message": message})
            
    final_table_summary = {
        "table_name": table_name,
        "results_by_type": consolidated_results,
        "total_cnes_processed_for_table": len(cnes_table_statuses)
    }

    return final_table_summary

@task
def create_and_send_final_report(all_tables_summaries: list):
    """
    Analisa os resultados consolidados de todas as tabelas e envia um relatório final
    para o Discord.
    """
    report_lines = []
    global_ok_count = 0
    global_vazia_count = 0
    global_banco_nao_encontrado_count = 0
    global_tabela_nao_encontrada_count = 0
    global_inesperado_count = 0
    
    total_tables_processed = len(all_tables_summaries)

    # Ordena as tabelas pelo nome para uma saída consistente
    sorted_summaries = sorted(all_tables_summaries, key=lambda x: x.get("table_name", "Z").lower())

    for table_summary in sorted_summaries:
        table_name = table_summary.get("table_name", "Tabela Desconhecida")
        results = table_summary.get("results_by_type", {})
        total_cnes_for_table = table_summary.get("total_cnes_processed_for_table", 0)

        # Atualiza contadores globais
        global_ok_count += len(results.get("ok_cnes", []))
        global_vazia_count += len(results.get("vazia_cnes", []))
        global_banco_nao_encontrado_count += len(results.get("banco_nao_encontrado_cnes", []))
        global_tabela_nao_encontrada_count += len(results.get("tabela_nao_encontrada_cnes", []))
        global_inesperado_count += len(results.get("inesperado_cnes", []))

        # Constrói a linha de resumo para CADA TABELA
        table_summary_line = f"**Tabela: {table_name}**"
        
        # Coleta os detalhes de problemas para a tabela atual
        table_issue_details = []
        
        if results.get("vazia_cnes"):
            cnes_str = ", ".join(sorted(results["vazia_cnes"]))
            table_issue_details.append(f"  - ⚠️ Vazias: CNES `{cnes_str}`")
        
        if results.get("banco_nao_encontrado_cnes"):
            cnes_str = ", ".join(sorted(results["banco_nao_encontrado_cnes"]))
            table_issue_details.append(f"  - 🔴 Banco não encontrado: CNES `{cnes_str}`")
            
        if results.get("tabela_nao_encontrada_cnes"):
            cnes_str = ", ".join(sorted(results["tabela_nao_encontrada_cnes"]))
            table_issue_details.append(f"  - 🔴 Tabela não encontrada: CNES `{cnes_str}`")
            
        if results.get("inesperado_cnes"):
            # Para erros inesperados, listamos o CNES e a mensagem detalhada se houver
            for err_info in results["inesperado_cnes"]:
                table_issue_details.append(f"  - 🔴 Inesperado: CNES `{err_info.get('cnes')}` - `{str(err_info.get('message', 'Erro desconhecido'))[:100]}...`")
        
        if table_issue_details:
            report_lines.append(table_summary_line)
            report_lines.extend(table_issue_details)
        else:
            report_lines.append(f"**Tabela: {table_name}** - Tudo OK ({total_cnes_for_table} CNES processados com dados).")
            
        report_lines.append("") 

    # --- Resumo Global ---
    overall_status_emoji = "🟢"
    if global_vazia_count > 0 or \
       global_banco_nao_encontrado_count > 0 or \
       global_tabela_nao_encontrada_count > 0 or \
       global_inesperado_count > 0:
        overall_status_emoji = "🔴" if (global_banco_nao_encontrado_count > 0 or global_tabela_nao_encontrada_count > 0 or global_inesperado_count > 0) else "⚠️"

    total_cnes_x_table = global_ok_count + global_vazia_count + \
                         global_banco_nao_encontrado_count + global_tabela_nao_encontrada_count + global_inesperado_count

    final_message_header = [
        "--- Relatório de Extração Vitacare ---",
        f"\n**Status Geral:** {overall_status_emoji} Conclusão da Pipeline",
        f"Total de Processamentos (CNES x Tabela): {total_cnes_x_table}",
        f"  - Com Dados (OK): {global_ok_count}",
        f"  - Vazias: {global_vazia_count}",
        f"  - Com Erros (Banco/Tabela/SQL): {global_banco_nao_encontrado_count + global_tabela_nao_encontrada_count + global_inesperado_count}",
        "\n--- Detalhes por Tabela ---",
        "" 
    ]

    final_report_message = "\n".join(final_message_header + report_lines)


    send_message(
        title="Status da Extração do Backup Vitacare",
        message=final_report_message,
        monitor_slug="data-ingestion",
        username="Prefect Vitacare Bot"
    )