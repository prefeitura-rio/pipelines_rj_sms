# -*- coding: utf-8 -*-
import fnmatch

import pandas as pd
from google.cloud import storage

from pipelines.datalake.extract_load.vitacare_gdrive.constants import constants
from pipelines.datalake.extract_load.vitacare_gdrive.utils import download_file
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

    def verify_columns_consistency(df_columns: list, found_columns: list) -> bool:
        consistency = len(set(found_columns).intersection(set(df_columns))) / len(df_columns)
        log(f"Consistency: {consistency}")
        return consistency >= 0.8

    dataframes = []
    for file_name in file_names:
        df = download_file(bucket, file_name)

        if df.empty:
            log(f"File {file_name} is empty. Skipping...")
            continue

        if len(dataframes) == 0:
            df.reset_index(inplace=True)
            dataframes.append(df)
            continue

        if verify_columns_consistency(df.columns.tolist(), dataframes[-1].columns.tolist()):
            df.reset_index(inplace=True)
            dataframes.append(df)
        else:
            log(
                f"One of the files {file_name} has columns that are not consistent with the previous files",  # noqa: E501
                level="error",
            )
            raise ValueError(
                f"One of the files {file_name} has columns that are not consistent with the previous files"  # noqa: E501
            )
    df_final = pd.concat(dataframes, ignore_index=True)

    return df_final
