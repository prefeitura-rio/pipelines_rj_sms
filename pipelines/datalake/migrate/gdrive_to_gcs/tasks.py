# -*- coding: utf-8 -*-
import os
import zipfile
from datetime import timedelta

from google.cloud import storage
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def get_fully_qualified_bucket_name(bucket_name: str, environment: str):
    log(f"Getting fully qualified bucket name for {bucket_name} in {environment}", level="info")

    if environment in ["prod", "local-prod"]:
        fq_bucket_name = bucket_name
    else:
        fq_bucket_name = f"{bucket_name}_{environment}"
    log(f"Fully qualified bucket name: {fq_bucket_name}", level="info")

    return fq_bucket_name


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def download_to_gcs(file_info: dict, bucket_name: str, folder_name: str):
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

    files = []
    # If the file is a zip, unzip it
    if file_info["path"].endswith(".zip"):
        try:
            with zipfile.ZipFile(file_info["path"], "r") as zip_ref:
                current_dir = os.path.dirname(file_info["path"])
                zip_ref.extractall(current_dir)

                # Remove the zip file
                os.remove(file_info["path"])
                log(f"Removed zip file {file_info['path']}", level="info")

                # Discover the files from the zip
                files = zip_ref.namelist()
                files = [os.path.join(current_dir, file) for file in files]
                log(f"Extracted files: {files}", level="info")
        except Exception as e:
            log(f"Error extracting zip file {file_info['path']}: {e}", level="error")
            raise e
    else:
        files = [file_info["path"]]

    # Upload to GCS
    bucket = storage.Client().bucket(bucket_name)

    blob_urls = []
    for file_path in files:
        blob_name = file_path.replace("./", f"{folder_name}/")
        log(f"Uploading file {file_path} to GCS as {blob_name}", level="info")
        blob = bucket.blob(blob_name)

        blob.upload_from_filename(file_path)
        log(f"Uploaded file {file_path} to GCS", level="info")

        # Remove the file from the local directory
        os.remove(file_path)
        log(f"Removed file {file_path} from local directory", level="info")
        blob_urls.append(blob.public_url)

    return blob_urls
