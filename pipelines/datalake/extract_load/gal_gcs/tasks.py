# -*- coding: utf-8 -*-
import fnmatch
import json
import os
import zipfile
from datetime import datetime, timedelta
from typing import List

import chardet
import pandas as pd
import pytz
from google.cloud import storage

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import upload_df_to_datalake
from pipelines.utils.time import from_relative_date


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def list_all_files(
    gcs_bucket: str, gcs_path_template: str, relative_target_date: str, environment: str
) -> List[str]:
    min_date = from_relative_date.run(relative_date=relative_target_date)
    # GCS Client
    client = storage.Client()
    blobs = client.list_blobs(gcs_bucket)

    # Filt
    files = []
    for blob in list(blobs):
        if pytz.UTC.localize(min_date) <= pytz.UTC.localize(pd.Timestamp(blob.time_created.date())):
            files.append(blob.name)

    # Verify path match the template
    matched_files = []
    for file in files:
        if fnmatch.fnmatch(file, gcs_path_template):
            matched_files.append(file)

    return matched_files


@task(max_retries=3, retry_delay=timedelta(minutes=1))
def process_file(gcs_bucket: str, gcs_file_path: str, dataset: str, table: str):
    # Download the file
    client = storage.Client()
    blob = client.bucket(gcs_bucket).blob(gcs_file_path)

    # Create directory if not exists
    os.makedirs(os.path.dirname(gcs_file_path), exist_ok=True)

    # Download the file
    blob.download_to_filename(gcs_file_path)

    # Unzip the file
    with zipfile.ZipFile(gcs_file_path, "r") as zip_ref:
        zip_ref.extractall(os.path.dirname(gcs_file_path))

    # List files in the directory ending with .csv
    csv_files = [
        os.path.join(os.path.dirname(gcs_file_path), f)
        for f in os.listdir(os.path.dirname(gcs_file_path))
        if f.endswith(".csv") and not f.startswith(".")
    ]

    # Discover the encoding of the file
    with open(csv_files[0], "rb") as f:
        raw_data = f.read()
        encoding = chardet.detect(raw_data)["encoding"].lower()

    # Read the file
    df = pd.read_csv(csv_files[0], encoding=encoding, sep=";", dtype=str)

    rows = []
    for i, r in enumerate(df.to_dict(orient="records")):
        rows.append(
            {
                "data": json.dumps(r, ensure_ascii=False),
                "file_path": gcs_file_path,
                "row_number": i + 1,
                "loaded_at": datetime.now(pytz.UTC).isoformat(),
            }
        )

    upload_df_to_datalake.run(
        df=pd.DataFrame(rows),
        table_id=table,
        dataset_id=dataset,
        partition_column="loaded_at",
        source_format="parquet",
    )

    return True
