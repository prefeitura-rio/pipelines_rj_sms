# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa E501
"""
Tasks to download data from Google Drive.
"""

from datetime import datetime, timedelta

from anytree import Node
from prefeitura_rio.pipelines_utils.logging import log
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
from tenacity import retry, stop_after_attempt, wait_fixed

from pipelines.utils.credential_injector import authenticated_task as task


@task
def get_files_from_folder(
    folder_id: str,
    last_modified_date: str = None,
    owner_email: str = None,
):
    if owner_email:
        log(f"Retrieving files from Google Drive folder {folder_id} by owner {owner_email}")
        return retrieve_files_from_gdrive_by_owner.run(
            folder_id=folder_id,
            last_modified_date=last_modified_date,
            owner_email=owner_email,
        )
    else:
        log(f"Retrieving files from Google Drive folder {folder_id} by root folder")
        return retrieve_files_from_gdrive_by_root_folder.run(
            folder_id=folder_id,
            last_modified_date=last_modified_date,
        )


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def get_folder_name(folder_id: str) -> str:
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

    # Query only the folder of the given folder_id
    folder = drive.CreateFile({"id": folder_id})
    folder.FetchMetadata()
    return folder["title"]


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def retrieve_files_from_gdrive_by_root_folder(
    folder_id: str,
    last_modified_date=None,
) -> list[dict]:
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

    @retry(stop=stop_after_attempt(7), wait=wait_fixed(2))
    def get_files_recursive(folder_id, last_modified_date, accumulated_path="."):
        log(f"FOLDER: {accumulated_path}")

        files = drive.ListFile({"q": f"'{folder_id}' in parents and trashed=false"}).GetList()

        for file in files:
            log(f"Looking at {accumulated_path}/{file['title']}")

            # File
            if file["mimeType"] != "application/vnd.google-apps.folder":
                modified_date = datetime.strptime(file["modifiedDate"], "%Y-%m-%dT%H:%M:%S.%fZ")
                if last_modified_date and modified_date < last_modified_date:
                    log(
                        f"File {file['title']} was last modified before {last_modified_date}, skipping",
                        level="info",
                    )
                    continue
                else:
                    files_list.append(
                        {
                            "path": f"{accumulated_path}/{file['title']}",
                            "id": file["id"],
                        }
                    )
            # Folder
            else:
                get_files_recursive(
                    file["id"], last_modified_date, f"{accumulated_path}/{file['title']}"
                )

    get_files_recursive(folder_id, last_modified_date)

    log(f"{len(files_list)} files found in Google Drive folder.", level="info")

    return files_list


@task(max_retries=3, retry_delay=timedelta(minutes=5))
def retrieve_files_from_gdrive_by_owner(
    folder_id: str,
    last_modified_date: str = None,
    owner_email: str = None,
) -> list[dict]:
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

    # ===============================
    # Creating Filters
    # ===============================
    # File filters
    file_filters = ["mimeType != 'application/vnd.google-apps.folder'", "trashed = false"]
    if last_modified_date:
        file_filters.append(f"modifiedDate >= '{last_modified_date.strftime('%Y-%m-%d')}'")
    if owner_email:
        file_filters.append(f"'{owner_email}' in owners")

    # Folder filters
    folder_filters = ["mimeType = 'application/vnd.google-apps.folder'", "trashed = false"]
    if owner_email:
        folder_filters.append(f"'{owner_email}' in owners")

    # ===============================
    # Searching
    # ===============================
    folders = drive.ListFile({"q": " and ".join(folder_filters)}).GetList()
    files = drive.ListFile({"q": " and ".join(file_filters)}).GetList()

    # ===============================
    # Indexing
    # ===============================
    folders_by_id = {folder["id"]: folder for folder in folders}
    folders_by_parent_id = {}
    for folder in folders:
        parent_id = folder["parents"][0]["id"] if folder["parents"] else None
        folders_by_parent_id[parent_id] = folders_by_parent_id.get(parent_id, []) + [folder]

    nodes_by_id = {folder["id"]: Node(folder["title"], id=folder["id"]) for folder in folders}

    # ===============================
    # Creating folder tree
    # ===============================
    log(f"Creating folder tree")
    for id, folder_node in nodes_by_id.items():
        folder_parent = (
            folders_by_id[id]["parents"][0] if len(folders_by_id[id]["parents"]) > 0 else None
        )
        if folder_parent:
            parent_node = nodes_by_id.get(folder_parent["id"])
            if parent_node:
                folder_node.parent = parent_node

    # take the subtree from folder_id

    log(f"Folder tree created")

    folder_node = nodes_by_id.get(folder_id)
    if folder_node:
        subtree = folder_node.descendants
        log(f"Subtree created for folder {folder_id}")
    else:
        log(f"Folder {folder_id} not found", level="error")
        return []

    _files = []
    for file in files:
        file_id = file["id"]
        file_folder_node = nodes_by_id.get(file["parents"][0]["id"])

        if file_folder_node and file_folder_node in subtree:
            folder_path = "/".join([node.name for node in file_folder_node.path])
            file_path = folder_path + "/" + file["title"]
            _files.append(
                {
                    "path": file_path,
                    "id": file_id,
                }
            )

    log(f"{len(_files)} files found in Google Drive folder.", level="info")

    return _files
