import fnmatch
import pandas as pd
import asyncio
import datetime
import pytz
import io
from google.cloud import storage

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log
from pipelines.datalake.extract_load.vitacare_gdrive.constants import constants


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
    file_names = file_names[:2]

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
        df = pd.read_csv(csv, sep=";")

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