# -*- coding: utf-8 -*-
# noqa E501
"""
Tasks to download data from Google Drive.
"""
from datetime import datetime, timedelta

import prefect
from prefeitura_rio.pipelines_utils.logging import log
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

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
def filter_files_by_date(files, start_date=None, end_date=None):
    """
    Filters a list of files based on their modified or created date.

    Args:
        files (list): A list of files to filter.
        start_date (str, optional): The start date for filtering. Defaults to None.
        end_date (str, optional): The end date for filtering. Defaults to None.

    Returns:
        list: A list of filtered files.

    Raises:
        ValueError: If the start_date or end_date has an invalid format.

    """

    if start_date:
        try:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError(
                "Invalid start_datetime format. Must be in the format %Y-%m-%d"
            ) from exc
    else:
        start_date = (prefect.context.get("scheduled_start_time") - timedelta(days=1)).date()

    if end_date:
        try:
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError as exc:
            raise ValueError("Invalid end_datetime format. Must be in the format %Y-%m-%d") from exc
    else:
        end_date = prefect.context.get("scheduled_start_time").date()

    filtered_files = []

    for file in files:
        modified_date = datetime.strptime(file["modifiedDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
        created_date = datetime.strptime(file["createdDate"], "%Y-%m-%dT%H:%M:%S.%fZ")

        last_update = modified_date if modified_date > created_date else created_date
        last_update = last_update.date()

        if start_date <= last_update <= end_date:
            filtered_files.append(file)

    log(
        f"{len(filtered_files)} files updated between {start_date} and {end_date}",
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


@task
def dowload_from_gdrive(
    folder_id: str,
    destination_folder: str,
    filter_type: str = "last_updated",
    filter_param: str | tuple = None,
) -> str:
    """
    Downloads files from a Google Drive folder based on specified filters.

    Args:
        folder_id (str): The ID of the Google Drive folder.
        destination_folder (str): The path to the destination folder where the downloaded files will be saved.
        filter_type (str, optional): The type of filter to apply. Defaults to "last_updated".
        filter_param (str | tuple, optional): The parameter(s) to use for filtering the files. Defaults to None.

    Returns:
        str: A dictionary indicating whether data was found and the downloaded files.

    """
    files = get_files_from_folder.run(folder_id=folder_id, file_extension="dbc")

    if filter_type == "last_updated":

        start_date, end_date = filter_param

        filtered_files = filter_files_by_date.run(
            files=files,
            start_date=start_date,
            end_date=end_date,
        )
    else:
        raise ValueError(f"Invalid filter type: {filter_type}")

    if filtered_files:
        downloaded_files = download_files.run(files=filtered_files, folder_path=destination_folder)
        return {"has_data": True, "files": downloaded_files}

    return {"has_data": False}
