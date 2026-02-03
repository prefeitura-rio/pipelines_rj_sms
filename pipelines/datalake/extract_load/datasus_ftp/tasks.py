# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Tasks for DataSUS pipelines
"""

import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.datasus_ftp.constants import (
    constants as datasus_constants,
)
from pipelines.datalake.extract_load.datasus_ftp.datasus.utils import (
    check_newest_file_version,
    fix_cnes_header,
)
from pipelines.datalake.utils.data_transformations import (
    add_flow_metadata,
    conform_header_to_datalake,
    convert_to_parquet,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import (
    create_partitions,
    download_from_ftp,
    upload_to_datalake,
)


@task(max_retries=2, timeout=timedelta(minutes=20), retry_delay=timedelta(seconds=10))
def extract_data_from_datasus(
    endpoint: str,
    download_path: str,
    download_newest: bool = False,
    file: str = None,
):
    """
    Extracts data from the DATASUS FTP server.

    Args:
        endpoint (str): The endpoint to extract data from.
        download_path (str): The path to download the extracted data.
        download_newest (bool, optional): Whether to download the newest file version. Defaults to False.
        file (str, optional): The specific file to download. If not provided, the default file for the endpoint will be used.

    Returns:
        list: A list of paths to the extracted files.
    """
    host = datasus_constants.DATASUS_FTP_SERVER.value
    directory = datasus_constants.DATASUS_ENDPOINT.value[endpoint]["file_path"]

    if endpoint == "cnes" and download_newest:
        response = check_newest_file_version(
            host=host,
            directory=directory,
            base_file_name=datasus_constants.DATASUS_ENDPOINT.value[endpoint]["base_file"],
        )
        file = response["file"]
    else:
        if file is None:
            log(
                f"File must be provided for endpoint {endpoint} if download_newest is False",
                level="error",
            )
            raise FAIL(f"File must be provided for endpoint {endpoint} if download_newest is False")

        log(f"Downloading file {file} from {datasus_constants.DATASUS_FTP_SERVER.value}")

    # Download file
    try:
        downloaded_file = download_from_ftp.run(
            host=host,
            directory=directory,
            file_name=file,
            output_path=download_path,
        )

        log(f"Files in download path: {os.listdir(download_path)}", level="debug")
        log(f"Downloaded file path: {downloaded_file}", level="debug")

    except Exception as e:
        log(f"Failed to download file {file}: {e}", level="error")
        raise FAIL(f"Failed to download file {file}") from e

    # Unzip file
    log("Unzipping file", level="info")
    shutil.unpack_archive(downloaded_file, download_path, "zip")

    if endpoint == "cbo":
        unziped_files = [
            f"{download_path}/DBF/{file}"
            for file in os.listdir(f"{download_path}/DBF")
            if file.startswith("CBO")
        ]
    else:
        unziped_files = [
            f"{download_path}/{file}" for file in os.listdir(download_path) if file.endswith(".csv")
        ]

    if len(unziped_files) == 0:
        log("No files found after unzip", level="error")
        raise FAIL("No files found after unzip")
    else:
        log("Files unziped successfully")
        return unziped_files


@task
def transform_data(files_path: list[str], endpoint: str):
    """
    Transforms data from the DATASUS FTP server.
    """

    raw_files = files_path

    if endpoint == "cbo":

        transformed_files = [
            convert_to_parquet(file_path=file, file_type="dbf", encoding="latin-1")
            for file in raw_files
        ]
        log("Files converted to parquet successfully")

        # Add metadata
        snapshot_date = datetime.fromtimestamp(os.path.getmtime(raw_files[0])).strftime("%Y-%m-%d")
        transformed_files = [
            add_flow_metadata(file_path=file, file_type="parquet", snapshot_date=snapshot_date)
            for file in transformed_files
        ]
        log("Metadata added successfully")

        transformed_files = [
            conform_header_to_datalake(file_path=file, file_type="parquet")
            for file in transformed_files
        ]
        log("Header conformed successfully")

    elif endpoint == "cnes":

        transformed_files = [fix_cnes_header(file) for file in raw_files]
        log("Header fixed successfully")

        transformed_files = [
            add_flow_metadata(file_path=file, file_type="csv", sep=";", parse_date_from="filename")
            for file in transformed_files
        ]
        log("Metadata added successfully")

        transformed_files = [
            convert_to_parquet(file_path=file, csv_sep=";", encoding="utf-8")
            for file in transformed_files
        ]
        log("Files converted to parquet successfully")

    else:
        log(f"Endpoint {endpoint} not found", level="error")
        raise FAIL(f"Endpoint {endpoint} not found")

    return transformed_files


@task
def create_many_partitions(files_path: list[str], partition_directory: str, endpoint: str):
    """
    Creates partitions for different tables.
    """

    if endpoint == "cbo":
        for file in files_path:
            shutil.copy(file, partition_directory)
        log("Partitions added successfully")

    elif endpoint == "cnes":
        for file in files_path:
            # retrieve file name from path
            file_name = Path(file).name.split(".")[0]

            # remove the last 6 digits representing the date
            table_id = file_name[:-6]
            partition_date = f"{file_name[-6:-2]}-{file_name[-2:]}"

            table_partition_folder = os.path.join(partition_directory, table_id)

            create_partitions.run(
                data_path=file,
                partition_directory=table_partition_folder,
                level="month",
                partition_date=partition_date,
            )
            log(f"Partition {table_partition_folder} created successfully", level="debug")
    else:
        log(f"Endpoint {endpoint} not found", level="error")
        raise FAIL(f"Endpoint {endpoint} not found")


@task
def upload_many_to_datalake(
    input_path: str,
    dataset_id: str,
    endpoint: str,
    source_format="parquet",
    if_exists="replace",
    if_storage_data_exists="replace",
    biglake_table=True,
    dataset_is_public=False,
):
    """
    Uploads different tables to data lake.
    """

    data_path = Path(input_path)

    if endpoint == "cbo":

        files = data_path.glob(f"*.{source_format}")

        for file in files:
            upload_to_datalake.run(
                input_path=file,
                dataset_id=dataset_id,
                table_id=file.name.split(".")[0].lower(),
                dump_mode="overwrite",
                source_format=source_format,
                if_exists=if_exists,
                if_storage_data_exists=if_storage_data_exists,
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
            )
        log("Files uploaded successfully", level="info")

    elif endpoint == "cnes":

        folders = data_path.glob("*")

        for folder in folders:
            upload_to_datalake.run(
                input_path=folder,
                dataset_id=dataset_id,
                table_id=folder.name,
                dump_mode="append",
                source_format=source_format,
                if_exists=if_exists,
                if_storage_data_exists=if_storage_data_exists,
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
            )
        log("Folders uploaded successfully", level="info")

    else:
        log(f"Endpoint {endpoint} not found", level="error")
        raise FAIL(f"Endpoint {endpoint} not found")
