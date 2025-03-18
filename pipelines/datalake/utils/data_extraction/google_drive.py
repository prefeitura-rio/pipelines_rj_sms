# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa E501
"""
Tasks to download data from Google Drive.
"""
import concurrent.futures
import os
from datetime import datetime, timedelta
from functools import partial

import prefect
from prefeitura_rio.pipelines_utils.logging import log
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_files_from_folder(
    folder_id,
    look_in_subfolders=False,
    file_extension="csv",
):
    """
    Retrieves a list of files from a specified Google Drive folder.

    Args:
        folder_id (str): The ID of the Google Drive folder.
        look_in_subfolders (bool, optional): Whether to search in subfolders. Defaults to False.
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
    files_list = []

    def get_files_recursive(folder_id):
        log(f"Querying folder {folder_id}")
        files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()

        for file in files:
            print(file)
            if file["title"].endswith(file_extension):
                files_list.append(file)
            if look_in_subfolders and file["mimeType"] == "application/vnd.google-apps.folder":
                get_files_recursive(file["id"])

    get_files_recursive(folder_id)

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

        # Check if file already exists in destination folder
        file_path = f"{folder_path}/{file['title']}"

        if os.path.exists(file_path):
            log(
                f"File {file['title']} already exists in {folder_path}, skipping download",
                level="info",
            )
            downloaded_files.append(file_path)
            continue

        file.GetContentFile(f"{folder_path}/{file['title']}")
        downloaded_files.append(f"{folder_path}/{file['title']}")

    log(f"{len(downloaded_files)} files downloaded to {folder_path}", level="info")
    log(f"Files downloaded: {downloaded_files}", level="debug")

    return downloaded_files


@task
def dowload_from_gdrive(
    folder_id: str,
    destination_folder: str,
    file_extension: str = "dbc",
    look_in_subfolders: bool = False,
    filter_type: str = "last_updated",
    filter_param: str | tuple = None,
) -> str:
    """
    Downloads files from a Google Drive folder and its subfolders based on specified filters.

    Args:
        folder_id (str): The ID of the Google Drive folder.
        destination_folder (str): The path to the destination folder where the downloaded files will be saved.
        filter_type (str, optional): The type of filter to apply. Defaults to "last_updated".
        filter_param (str | tuple, optional): The parameter(s) to use for filtering the files. Defaults to None.

    Returns:
        str: A dictionary indicating whether data was found and the downloaded files.

    """

    files = get_files_from_folder.run(
        folder_id=folder_id,
        file_extension=file_extension,
        look_in_subfolders=look_in_subfolders,
    )

    if filter_type == "last_updated":
        if filter_param:
            filtered_files = filter_files_by_date.run(
                files=files,
                start_date=filter_param[0],
                end_date=filter_param[1],
            )
        else:
            filtered_files = filter_files_by_date.run(files=files)
    else:
        raise ValueError(f"Invalid filter type: {filter_type}")

    if filtered_files:
        downloaded_files = download_files.run(files=filtered_files, folder_path=destination_folder)
        return {"has_data": True, "files": downloaded_files}

    return {"has_data": False}


def upload_folder_to_gdrive(folder_path: str, parent_folder_id: str, max_workers: int = 20):
    """
    Uploads an entire folder to Google Drive, preserving the folder structure.
    Avoids creating duplicate folders and replaces existing files.

    Args:
        folder_path (str): The path to the folder to be uploaded.
        parent_folder_id (str): The ID of the Google Drive folder where the contents will be uploaded.

    Returns:
        None
    """
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

    def get_or_create_folder(name, parent_id):
        query = f"title='{name}' and '{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        existing_folders = drive.ListFile({"q": query}).GetList()

        if existing_folders:
            return existing_folders[0]

        folder_metadata = {
            "title": name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [{"id": parent_id}],
        }
        folder = drive.CreateFile(folder_metadata)
        folder.Upload()
        return folder

    def upload_or_update_file(file_path, parent_id):
        file_name = os.path.basename(file_path)
        query = f"title='{file_name}' and '{parent_id}' in parents and trashed=false"
        existing_files = drive.ListFile({"q": query}).GetList()

        if existing_files:
            file = existing_files[0]
        else:
            file = drive.CreateFile({"parents": [{"id": parent_id}]})

        file.SetContentFile(file_path)
        file.Upload()

    def upload_folder(local_path, parent_id):
        items = os.listdir(local_path)
        files_to_upload = []

        for item in items:
            item_path = os.path.join(local_path, item)
            if os.path.isfile(item_path):
                files_to_upload.append((item_path, parent_id))
            elif os.path.isdir(item_path):
                subfolder = get_or_create_folder(item, parent_id)
                upload_folder(item_path, subfolder["id"])

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            list(executor.map(lambda x: upload_or_update_file(*x), files_to_upload))

    # Start uploading directly to the provided parent_folder_id
    upload_folder(folder_path, parent_folder_id)

    log(
        f"Folder {folder_path} contents uploaded to Google Drive folder {parent_folder_id} in parallel, preserving structure and updating existing files",
        level="info",
    )
