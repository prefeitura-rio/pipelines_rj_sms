# -*- coding: utf-8 -*-
import fnmatch
from datetime import timedelta
import pandas as pd
from google.cloud import storage

from pipelines.datalake.extract_load.vitacare_gdrive.constants import constants
from pipelines.datalake.extract_load.vitacare_gdrive.utils import safe_download_file, download_file
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
def get_most_recent_schema(file_pattern: str, environment: str) -> pd.DataFrame:
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

    df = safe_download_file(bucket, most_recent_file)
    return df.columns.tolist()


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def upload_consistent_files(
    file_name: str,
    expected_schema: list,
    environment: str,
    dataset_id: str,
    table_id: str,
    inadequency_threshold: float = 0.2,
    use_safe_download_file: bool = True,
) -> pd.DataFrame:
    client = storage.Client()
    bucket_name = constants.GCS_BUCKET.value[environment]
    bucket = client.bucket(bucket_name)
    
    # Download file
    try:
        if use_safe_download_file:
            df = safe_download_file(bucket, file_name)
        else:
            df = download_file(bucket, file_name)
    except Exception as e:
        log(f"Error downloading file {file_name}: {e}", level="error")
        return {
            "file_name": file_name,
            "inadequency_index": 1,
            "missing_columns": [],
            "extra_columns": [],
            "loaded": False,
        }

    # Check if the file is empty
    if df is None or df.empty:
        log(f"File {file_name} is empty. Skipping...", level="error")
        return None

    # Calculate missing columns
    missing_columns = set(expected_schema) - set(df.columns.tolist())
    log(f"Missing columns: {missing_columns}. Filling with None.")

    # Fill missing columns with None
    for column in missing_columns:
        df[column] = None

    # Calculate extra columns
    extra_columns = set(df.columns.tolist()) - set(expected_schema)
    log(f"Extra columns: {extra_columns}. Dropping them.")

    # Drop extra columns
    df = df.drop(columns=extra_columns)

    inadequency_index = (len(missing_columns) + len(extra_columns)) / len(expected_schema)

    if inadequency_index < inadequency_threshold:
        upload_df_to_datalake.run(
            df=df,
            partition_column="_loaded_at",
            dataset_id=dataset_id,
            table_id=table_id,
            source_format="parquet",
            dump_mode="append",
            if_exists="append",
        )

    return {
        "file_name": file_name,
        "inadequency_index": inadequency_index,
        "missing_columns": missing_columns,
        "extra_columns": extra_columns,
        "loaded": inadequency_index < inadequency_threshold,
    }

@task(max_retries=3, retry_delay=timedelta(seconds=30))
def report_inadequency(file_pattern: str, reports: list[dict]):
    loaded_reports = [report for report in reports if report["loaded"]]
    not_loaded_reports = [report for report in reports if not report["loaded"]]

    message = []

    message.append(f"## Padrão de Arquivos: `{file_pattern}`")
    message.append(f"- Total de arquivos: {len(reports)}")
    message.append(f"- Total de arquivos carregados: {len(loaded_reports)}")
    message.append(f"- Total de arquivos não carregados: {len(not_loaded_reports)}")

    for report in not_loaded_reports:
        message.append(f"- Arquivo: {report['file_name']}")
        message.append(f"  - Inadequência: {report['inadequency_index']}")
        message.append(f"  - Colunas ausentes: {report['missing_columns']}")
        message.append(f"  - Colunas extras: {report['extra_columns']}")

    if len(not_loaded_reports) > 0:
        log("\n".join(message), level="error")
    else:
        log("\n".join(message))
