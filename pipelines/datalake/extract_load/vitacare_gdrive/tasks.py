# -*- coding: utf-8 -*-
import os
import zipfile
from google.cloud import storage
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

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
    gauth = GoogleAuth(
        settings={
            "client_config_backend": "service",
            "service_config": {
                "client_json_file_path": "/tmp/credentials.json",
            },
        }
    )
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)

    # Download file from Google Drive
    os.makedirs(os.path.dirname(file_info["path"]), exist_ok=True)

    file = drive.CreateFile({"id": file_info["id"]})
    file.GetContentFile(file_info["path"])
    log(f"Downloaded file {file_info['path']} from Google Drive", level="info")
    
    # If the file is a zip, unzip it
    if file_info["path"].endswith(".zip"):
        with zipfile.ZipFile(file_info["path"], "r") as zip_ref:
            zip_ref.extractall(os.path.dirname(file_info["path"]))

        # Remove the zip file
        os.remove(file_info["path"])
        log(f"Removed zip file {file_info['path']}", level="info")
        file_info["path"] = file_info["path"].replace(".zip", ".csv")

    # Upload to GCS
    bucket_name = gdrive_constants.GCS_BUCKET.value[environment]
    bucket = storage.Client().bucket(bucket_name)

    blob_name = file_info["path"].replace("./", f"{ap}/")
    blob = bucket.blob(blob_name)

    blob.upload_from_filename(file_info["path"])
    log(f"Uploaded file {file_info['path']} to GCS", level="info")

    # Remove the file from the local directory
    os.remove(file_info["path"])
    log(f"Removed file {file_info['path']} from local directory", level="info")
    return blob.public_url
