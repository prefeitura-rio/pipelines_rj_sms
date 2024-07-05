# -*- coding: utf-8 -*-
"""
SIH dumping tasks
"""
import os
from typing import List

from prefeitura_rio.pipelines_utils.logging import log
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.datalake.utils.data_extraction.google_drive import (
    download_files,
    filter_files_by_date,
    get_files_from_folder,
)
from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
    convert_to_parquet,
)

@task
def dowload_from_gdrive(folder_id: str, destination_folder: str) -> str:
    """
    Downloads the file from Google Drive.

    Args:
        file_id (str): The file ID in Google Drive.
        file_path (str): The path to save the downloaded file.

    Returns:
        str: The path of the downloaded file.
    """

    files = get_files_from_folder.run(folder_id=folder_id, file_extension="dbc")

    last_24h_files = filter_files_by_date.run(
        files=files,
        start_datetime="2024-07-04T00:00:00.000Z",  # TODO: remove variable
        end_datetime="2024-07-04T22:19:34.153Z",  # TODO: remove variable
    )

    downloaded_files = download_files.run(files=last_24h_files, folder_path=destination_folder)

    return downloaded_files


@task
def transform_data(files_path: List[str]) -> List[str]:
    """
    Transforms the data files in the given list of file paths.

    Args:
        files_path (List[str]): A list of file paths to be transformed.

    Returns:
        List[str]: A list of transformed file paths.

    """
    transformed_files = []

    for file in files_path:

        log(f"Transforming file: {os.path.basename(file)}")

        parquet_file_path = convert_to_parquet(file_path=file, file_type="dbc", encoding="latin1")

        parquet_file_path = conform_header_to_datalake(
            file_path=parquet_file_path,
            file_type="parquet",
        )

        # standardize the file name
        file_path, file_name = os.path.split(parquet_file_path)

        new_file_name = f"{file_name[:4]}_20{file_name[4:6]}-{file_name[6:8]}-01.parquet"
        new_parquet_file_path = os.path.join(file_path, new_file_name)

        os.rename(parquet_file_path, new_parquet_file_path)

        log(f"File transformed to: {os.path.basename(new_file_name)}")

        transformed_files.append(new_parquet_file_path)

    return transformed_files
