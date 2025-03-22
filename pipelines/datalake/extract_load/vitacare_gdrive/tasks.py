# -*- coding: utf-8 -*-
import datetime
import fnmatch
import io

import pandas as pd
import pytz
from google.cloud import storage
from tenacity import retry, stop_after_attempt, wait_fixed
from unidecode import unidecode

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

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
    def download_file(file_name):
        blob = bucket.get_blob(file_name)
        size_in_bytes = blob.size
        size_in_mb = size_in_bytes / (1024 * 1024)

        log(f"Beginning Download of {file_name} with {size_in_mb:.1f} MB")
        csv_text = blob.download_as_text(encoding="utf-8")
        csv_file = io.StringIO(csv_text)

        first_line = csv_text.splitlines()[0]

        if len(first_line.split(",")) > len(first_line.split(";")):
            sep = ","
        else:
            sep = ";"

        df = pd.read_csv(csv_file, sep=sep, dtype=str)

        # Remove prohibited symbols in columns in the BigQuery table
        cleaned_columns = []
        for column in df.columns:
            cleaned_column = (
                column.replace("(", "").replace(")", "").replace(" ", "_").replace("-", "_").lower()
            )
            cleaned_column = unidecode(cleaned_column)
            cleaned_columns.append(cleaned_column)
        df.columns = cleaned_columns

        df["_source_file"] = file_name
        df["_extracted_at"] = blob.updated.astimezone(tz=pytz.timezone("America/Sao_Paulo"))
        df["_loaded_at"] = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

        log(f"Finishing Download of {file_name}")
        return df

    dataframes = []
    for file_name in file_names:
        dataframes.append(download_file(file_name))

    df_final = pd.concat(dataframes, ignore_index=True)

    log(df_final.head())

    return df_final
