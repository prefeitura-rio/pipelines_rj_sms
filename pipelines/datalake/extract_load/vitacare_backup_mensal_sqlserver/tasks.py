# -*- coding: utf-8 -*-
"""
Tasks para extração e transformação de dados do Vitacare Historic SQL Server
"""
from datetime import datetime, timedelta

import pandas as pd
import pytz
from sqlalchemy import create_engine

from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.logger import log


@task
def get_all_cnes_codes() -> list:
    """Retorna a lista de todos os códigos CNES a serem processados."""
    log("Iniciando a busca por todos os códigos CNES.")
    try:
        codes = vitacare_constants.cnes_codes.value
        log(f"Foram encontrados {len(codes)} códigos CNES para processamento.")
        return codes
    except AttributeError:
        log(
            "Erro: 'cnes_codes' não encontrado ou não é um Enum com .value em vitacare_constants. Verifique a importação e definição.",
            level="error",
        )
        raise ValueError("Lista de CNES não pôde ser carregada a partir de vitacare_constants.")
    except Exception as e:
        log(f"Erro inesperado ao buscar códigos CNES: {e}", level="error")
        raise


@task(nout=2)
def generate_extraction_cartesian_product(
    cnes_codes: list, tables_to_extract: list
) -> tuple[list, list]:
    """
    Gera o produto cartesiano entre códigos CNES e tabelas para mapeamento.
    Retorna duas listas: uma de códigos CNES e uma de nomes de tabelas, alinhadas para o map.
    """
    log(
        f"Gerando produto cartesiano para {len(cnes_codes)} CNES e {len(tables_to_extract)} tabelas."
    )
    cnes_inputs = []
    table_inputs = []
    for cnes in cnes_codes:
        for table in tables_to_extract:
            cnes_inputs.append(cnes)
            table_inputs.append(table)
    log(f"Produto cartesiano gerado com {len(cnes_inputs)} combinações.")
    return cnes_inputs, table_inputs


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def extract_and_transform_table(
    db_host: str,
    db_port: str,
    db_user: str,
    db_password: str,
    db_schema: str,
    db_table: str,
    cnes_code: str,
) -> pd.DataFrame:
    full_table_name = f"{db_schema}.{db_table}"
    log(f"Attempting to download data from {full_table_name} for CNES: {cnes_code}")

    db_name = f"vitacare_historic_{cnes_code}"
    try:
        connection_string = (
            f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
        engine = create_engine(connection_string)

        df = pd.read_sql(f"SELECT * FROM {full_table_name}", engine)
        log(f"Successfully downloaded {len(df)} rows from {full_table_name}.")

        now = datetime.now(tz=pytz.timezone("America/Sao_Paulo"))
        df["extracted_at"] = now
        df["id_cnes"] = cnes_code

        df.columns = remove_columns_accents(df)
        log(f"Transformed DataFrame for {full_table_name} from CNES {cnes_code}.")

        # Essas 2 tabelas possuem uma columa varbinary e isso gera um erro ao tentar passar para csv
        tables_with_ut_id = ["PACIENTES", "ATENDIMENTOS"]

        if db_table.upper() in tables_with_ut_id and "_ut_id" in df.columns:

            def clean_ut_id(val):
                if isinstance(val, bytes):
                    try:
                        decoded_val = val.decode("utf-16-le", errors="ignore")
                    except UnicodeDecodeError:
                        decoded_val = val.decode("latin-1", errors="ignore")

                    clean_str = (
                        decoded_val.replace("\x00", "")
                        .replace("\n", " ")
                        .replace("\r", " ")
                        .replace("\t", " ")
                    )
                    if any(ord(c) < 32 and c not in (" ",) for c in clean_str):
                        return val.hex()
                    return clean_str.strip()
                elif isinstance(val, str):
                    if val.startswith("b'") and val.endswith("'"):
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
                            return byte_repr.hex()
                        return clean_str.strip()

                    clean_str = (
                        val.replace("\x00", "")
                        .replace("\n", " ")
                        .replace("\r", " ")
                        .replace("\t", " ")
                    )
                    return clean_str.strip()

                return (
                    str(val)
                    .replace("\x00", "")
                    .replace("\n", " ")
                    .replace("\r", " ")
                    .replace("\t", " ")
                    .strip()
                )

            df["_ut_id"] = df["_ut_id"].apply(clean_ut_id)
            log(f"'_ut_id' in {full_table_name} cleaned from NUL bytes and other control chars.")

        for col in df.select_dtypes(include=["object", "string"]).columns:
            current_col_data = df[col].astype(str)
            current_col_data = current_col_data.str.replace(r"[\n\r\t\x00]+", " ", regex=True)
            current_col_data = current_col_data.str.replace(r"[^ -~]+", " ", regex=True)
            current_col_data = current_col_data.str.replace('"', '""', regex=False)
            df[col] = '"' + current_col_data + '"'

        log(
            f"All string columns for {full_table_name} prepared for strict CSV compatibility "
            "(forced quoting)."
        )

        df = df.astype(str)
        log(f"Converted all columns to string type for {full_table_name}.")

        # Tratamento para a coluna 'acto_id'
        for col_name in df.columns:
            if col_name.lower() == "acto_id":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
                df[col_name] = df[col_name].astype(pd.Int64Dtype())
                log(f"Column '{col_name}' in {full_table_name} converted to nullable integer.")

        return df

    except Exception as e:
        log(
            f"Error downloading or transforming data from {full_table_name} "
            f"(CNES: {cnes_code}): {e}",
            level="error",
        )
        raise


@task
def get_tables_to_extract() -> list:
    return [
        "ALERGIAS",
        "ATENDIMENTOS",
        "CONDICOES",
        "ENCAMINHAMENTOS",
        "EQUIPES",
        "HIV",
        "INDICADORES",
        "PACIENTES",
        "PRENATAL",
        "PRESCRICOES",
        "PROCEDIMENTOS_CLINICOS",
        "PROFISSIONAIS",
        "SAUDECRIANCA",
        "SAUDEMULHER",
        "SIFILIS",
        "SOLICITACAO_EXAMES",
        "TESTESRAPIDOS",
        "TUBERCULOSE",
        "UNIDADE",
        "VACINAS",
    ]


@task
def build_bq_table_name(table_name: str) -> str:
    bq_table_name = f"{table_name.lower()}_historic"
    log(f"Built BigQuery table name: {bq_table_name} for source table: {table_name}")
    return bq_table_name
