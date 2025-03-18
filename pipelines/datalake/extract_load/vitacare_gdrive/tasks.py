# -*- coding: utf-8 -*-
import os

from google.cloud import storage

from pipelines.datalake.extract_load.vitacare_gdrive.constants import (
    constants as gdrive_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def get_folder_id(ap: str):
    return gdrive_constants.GDRIVE_FOLDER_STRUCTURE.value[ap]


@task
def download_to_gcs(file_info: dict, ap: str, environment: str):
    # Download file from Google Drive
    file = file_info["file"]
    os.makedirs(os.path.dirname(file_info["path"]), exist_ok=True)
    file.GetContentFile(file_info["path"])
    log(f"Downloaded file {file_info['path']} from Google Drive", level="info")

    # Upload to GCS
    bucket_name = gdrive_constants.GCS_BUCKET.value[environment]
    bucket = storage.Client().bucket(bucket_name)

    blob_name = file_info["path"].replace("./", f"{ap}/")
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(file_info["path"])
    log(f"Uploaded file {file_info['path']} to GCS", level="info")

    return blob.public_url
