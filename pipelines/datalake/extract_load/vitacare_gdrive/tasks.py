# -*- coding: utf-8 -*-
import asyncio
import datetime
import fnmatch
import io

import pandas as pd
import pytz
from google.cloud import storage

from pipelines.datalake.extract_load.vitacare_gdrive.constants import constants
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
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


@task
def join_csv_files(file_names: list[str], environment: str) -> pd.DataFrame:
    client = storage.Client()
    bucket_name = constants.GCS_BUCKET.value[environment]
    bucket = client.bucket(bucket_name)

    log(f"Downloading {len(file_names)} files")

    async def download_file(file_name):
        blob = bucket.get_blob(file_name)
        size_in_bytes = blob.size
        size_in_mb = size_in_bytes / (1024 * 1024)

        log(f"Beginning Download of {file_name} with {size_in_mb:.1f} MB")
        csv = io.StringIO(blob.download_as_text())
        df = pd.read_csv(csv, dtype=str)

        # Remove prohibited symbols in columns in the BigQuery table
        df.columns = df.columns.str.replace(r"[^\x00-\x7F]+", "", regex=True)

        df["_source_file"] = file_name
        df["_extracted_at"] = blob.updated.astimezone(tz=pytz.timezone("America/Sao_Paulo"))
        df["_loaded_at"] = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

        log(f"Finishing Download of {file_name}")
        return df

    async def download_all_files():
        tasks = [download_file(name) for name in file_names]
        return await asyncio.gather(*tasks)

    dataframes = asyncio.run(download_all_files())

    df_final = pd.concat(dataframes, ignore_index=True)

    return df_final
