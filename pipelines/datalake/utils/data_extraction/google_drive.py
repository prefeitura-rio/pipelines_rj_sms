# -*- coding: utf-8 -*-
"""
Tasks to download data from Google Drive.
"""
from datetime import datetime, timedelta

import prefect
from prefeitura_rio.pipelines_utils.logging import log
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from pytz import timezone

from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_files_from_folder(folder_id, file_extension="csv"):
    """
    Retrieves a list of files from a specified Google Drive folder.

    Args:
        folder_id (str): The ID of the Google Drive folder.
        file_extension (str, optional): The file extension to filter the files. Defaults to "csv".

    Returns:
        list: A list of files in the specified folder with the given file extension.
    """
    log("Authenticating with Google Drive")
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

    log("Querying root folder")
    files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()

    files_list = []

    for file in files:
        if file["title"].endswith(file_extension):
            files_list.append(file)

    log(f"{len(files_list)} files found in Google Drive folder.", level="info")
    log(f"Files: {files_list}", level="debug")

    return files_list


@task
def filter_files_by_date(files, start_datetime=None, end_datetime=None):
    """
    Filters a list of files based on their modified or created dates.

    Args:
        files (list): A list of files to be filtered.
        start_datetime (str, optional): The start datetime for filtering. Defaults to None.
        end_datetime (str, optional): The end datetime for filtering. Defaults to None.

    Returns:
        list: A list of filtered files.

    Raises:
        ValueError: If the start_datetime or end_datetime has an invalid format.

    """
    if start_datetime:
        try:
            start_datetime = datetime.strptime(start_datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError as exc:
            raise ValueError(
                "Invalid start_datetime format. Must be in the format %Y-%m-%dT%H:%M:%S.%fZ"
            ) from exc
    else:
        start_datetime = prefect.context.get("scheduled_start_time") - timedelta(days=1)

    if end_datetime:
        try:
            end_datetime = datetime.strptime(end_datetime, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError as exc:
            raise ValueError(
                "Invalid end_datetime format. Must be in the format %Y-%m-%dT%H:%M:%S.%fZ"
            ) from exc
    else:
        end_datetime = prefect.context.get("scheduled_start_time")

    filtered_files = []

    for file in files:
        modified_date = datetime.strptime(file["modifiedDate"], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(
            timezone("UTC")
        )
        created_date = datetime.strptime(file["createdDate"], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(
            timezone("UTC")
        )

        last_update = modified_date if modified_date > created_date else created_date

        if start_datetime <= last_update <= end_datetime:
            filtered_files.append(file)

    log(
        f"{len(filtered_files)} files found for dates between {start_datetime} and {end_datetime}",
        level="info",
    )

    return filtered_files


@task
def download_files(files, folder_path):
    """
    Downloads a list of files from Google Drive to a specified folder.

    Args:
        files (list): A list of files to be downloaded.
        folder_path (str): The path to save the downloaded files.

    Returns:
        list: A list of paths of the downloaded files.
    """
    log(f"Downloading {len(files)} files to {folder_path}")

    downloaded_files = []

    for file in files:
        file.GetContentFile(f"{folder_path}/{file['title']}")
        downloaded_files.append(f"{folder_path}/{file['title']}")

    log(f"{len(downloaded_files)} files downloaded to {folder_path}", level="info")
    log(f"Files downloaded: {downloaded_files}", level="debug")

    return downloaded_files
