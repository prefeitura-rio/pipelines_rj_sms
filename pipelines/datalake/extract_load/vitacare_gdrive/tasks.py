# -*- coding: utf-8 -*-
import fnmatch
from datetime import timedelta

import chardet
import pandas as pd
from google.cloud import storage

from pipelines.datalake.extract_load.vitacare_gdrive.constants import constants
from pipelines.datalake.extract_load.vitacare_gdrive.utils import (
    download_file,
    fix_column_name,
    format_bytes,
    get_file_size,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.utils.tasks import upload_df_to_datalake


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def find_all_file_names_from_pattern(file_pattern: str, environment: str):
    client = storage.Client()

    bucket_name = constants.GCS_BUCKET.value[environment]
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()

    log(f"Using pattern: {file_pattern}")
    files = []
    for blob in blobs:
        if fnmatch.fnmatch(blob.name, file_pattern):
            files.append(blob.name)

    log(f"{len(files)} files were found. Their names:\n - " + "\n - ".join(files))
    return files


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def get_most_recent_schema(file_pattern: str, environment: str) -> list:
    client = storage.Client()
    bucket_name = constants.GCS_BUCKET.value[environment]
    bucket = client.bucket(bucket_name)

    blobs = bucket.list_blobs()
    files = []
    for blob in blobs:
        if fnmatch.fnmatch(blob.name, file_pattern):
            files.append(blob)
    log(f"{len(files)} files were found")

    files.sort(key=lambda x: x.updated)
    most_recent_file = files[-1].name
    log(f"Most recent file: {most_recent_file}")

    # Baixa o arquivo mais recente
    csv_file, detected_separator, _ = download_file(bucket, most_recent_file, extra_safe=True)

    try:
        # Pega a primeira linha
        data = csv_file.readline()
        first_line = data
        if not isinstance(data, str):
            detected_encoding = chardet.detect(data)["encoding"]
            first_line = data.decode(detected_encoding)
        raw_columns = first_line.split(detected_separator)
        # Padroniza colunas
        columns = [fix_column_name(col) for col in raw_columns]
        log(f"Found {len(columns)} column(s): {columns}")
        return columns
    finally:
        # Garante que o file handle está fechado
        # Como é um TemporaryFile, ele também apaga o arquivo automaticamente
        csv_file.close()


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def upload_consistent_files(
    file_name: str,
    expected_schema: list,
    environment: str,
    dataset_id: str,
    table_id: str,
    inadequacy_threshold: float = 0.2,
    use_safe_download_file: bool = True,
) -> pd.DataFrame:
    client = storage.Client()
    bucket_name = constants.GCS_BUCKET.value[environment]
    bucket = client.bucket(bucket_name)

    # Download file
    try:
        csv_file, detected_separator, metadata_columns = download_file(
            bucket, file_name, extra_safe=use_safe_download_file
        )
    except Exception as e:
        log(f"Error downloading file {file_name}: {e}", level="error")
        return {
            "file_name": file_name,
            "inadequacy_index": 1,
            "missing_columns": [],
            "extra_columns": [],
            "loaded": False,
        }

    file_size_bytes = get_file_size(csv_file)
    # Se o arquivo está vazio
    if file_size_bytes <= 0:
        log(f"Downloaded file '{file_name}' is empty. Skipping...", level="error")
        return None
    else:
        log(f"Downloaded file '{file_name}' has size {format_bytes(file_size_bytes)}")

    # Quantidade de linhas do CSV a serem lidas a cada iteração
    # Isso é importante porque alguns arquivos pesam >1GB e estouram a memória
    # Experimentalmente:
    # - Arquivo de ~910 MB = 23 chunks de 100k linhas, ou 12 chunks de 200k linhas
    lines_per_chunk = 200_000
    csv_reader = None
    try:
        # Cria um leitor pedaço a pedaço do arquivo CSV, mas ainda não carrega nada
        csv_reader = pd.read_csv(
            csv_file, sep=detected_separator, dtype=str, encoding="utf-8", chunksize=lines_per_chunk
        )
    except pd.errors.ParserError:
        log("Error reading CSV file", level="error")
        return pd.DataFrame()
    except UnicodeDecodeError:
        # Se tivemos erro de decoding, o arquivo não está em UTF-8;
        # tenta com CP-1252, superset de ISO-8859-1/Latin-1
        csv_reader = pd.read_csv(
            csv_file,
            sep=detected_separator,
            dtype=str,
            encoding="cp1252",
            chunksize=lines_per_chunk,
        )

    # Colunas inesperadas que serão modificadas abaixo
    missing_columns = None
    extra_columns = None
    inadequacy_index = None

    # Iteramos por cada pedaço do CSV
    for i, chunk in enumerate(csv_reader):
        # Cria um dataframe a partir do chunk
        df = pd.DataFrame(chunk)
        log(f"Reading chunk #{i+1} (at most {lines_per_chunk} lines)")

        # Padroniza as colunas
        df.columns = [fix_column_name(col) for col in df.columns]
        if inadequacy_index is None:
            log(f"Found {len(df.columns)} column(s).")

        if missing_columns is None:
            # Detecta colunas faltantes
            missing_columns = set(expected_schema) - set(df.columns.tolist())
            log(f"Missing column(s) ({len(missing_columns)}): {list(missing_columns)}")
        log(f"Filling {len(missing_columns)} missing column(s) with None.")
        # Preenche colunas faltantes com None
        for column in missing_columns:
            df[column] = None

        if extra_columns is None:
            # Detecta colunas extras
            extra_columns = set(df.columns.tolist()) - set(expected_schema)
            log(f"Extra column(s) ({len(extra_columns)}): {list(extra_columns)}.")
        log(f"Dropping {len(extra_columns)} extra column(s).")
        # Remove colunas extras
        df = df.drop(columns=extra_columns)

        # Adiciona metadados
        for column, value in metadata_columns.items():
            df[column] = value

        # Cria um índice de 0 a [tamanho do dataframe]
        # Se já processamos algum chunk anteriormente, então o índice vai de
        # `lines_per_chunk` até `lines_per_chunk`+[tamanho]
        # range(a, b) inclui `a` mas exclui `b`, então não precisamos de len()-1
        start_index = i * lines_per_chunk
        df["indice"] = range(start_index, start_index + len(df))

        if inadequacy_index is None:
            # Calcula o quão inesperado é o formato recebido
            inadequacy_index = (len(missing_columns) + len(extra_columns)) / len(expected_schema)
        # Se suficientemente adequado, faz upload
        if inadequacy_index < inadequacy_threshold:
            log("Uploading chunk to datalake.")
            upload_df_to_datalake.run(
                df=df,
                partition_column="_loaded_at",
                dataset_id=dataset_id,
                table_id=table_id,
                source_format="parquet",
                dump_mode="append",
                if_exists="append",
            )
        else:
            # Não precisamos continuar iterando por todos os pedaços se
            # sabemos que o schema está inadequado
            log(
                f"Inadequacy index ({inadequacy_index:.2f}) "
                + f"over threshold ({inadequacy_threshold:.2f}); aborting"
            )
            break

    log("Finished reading all chunks.")

    # Garante que o file handle foi devidamente fechado
    # Como é um TemporaryFile, ele também apaga o arquivo automaticamente
    csv_file.close()

    return {
        "file_name": file_name,
        "inadequacy_index": inadequacy_index,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "loaded": inadequacy_index < inadequacy_threshold,
    }


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def report_inadequacy(file_pattern: str, reports: list[dict]):
    loaded_reports = [report for report in reports if report["loaded"]]
    not_loaded_reports = [report for report in reports if not report["loaded"]]

    message = []

    message.append(f"## Padrão de Arquivos: `{file_pattern}`")
    message.append(f"- Total de arquivos: {len(reports)}")
    message.append(f"- Total de arquivos carregados: {len(loaded_reports)}")
    message.append(f"- Total de arquivos não carregados: {len(not_loaded_reports)}")

    for report in not_loaded_reports:
        message.append(f"- Arquivo: {report['file_name']}")
        message.append(f"  - Inadequência: {report['inadequacy_index']:.2f}")
        message.append(f"  - Colunas ausentes: {report['missing_columns']}")
        message.append(f"  - Colunas extras: {report['extra_columns']}")

    if len(not_loaded_reports) > 0:
        log("\n".join(message), level="error")
    else:
        log("\n".join(message))
