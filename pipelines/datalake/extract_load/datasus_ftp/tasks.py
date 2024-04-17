# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Taks for DataSUS pipelines
"""
from datetime import timedelta
import subprocess
import os

from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import download_ftp
from pipelines.datalake.extract_load.datasus_ftp.constants import constants as datasus_constants
from pipelines.datalake.extract_load.datasus_ftp.datasus.utils import (check_newest_file_version, fix_cnes_header, add_flow_metadata)


@task(max_retries=2, timeout=timedelta(minutes=10), retry_delay=timedelta(seconds=10))
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

    # Define file to download
    if download_newest:
        response = check_newest_file_version(
            host=host,
            directory=directory,
            base_file_name=datasus_constants.DATASUS_ENDPOINT.value[endpoint]["base_file"],
        )
        file = response["file"]
    else:
        if file is None:
            try:
                file = datasus_constants.DATASUS_ENDPOINT.value[endpoint]["file"]
            except KeyError as e:
                log(
                    f"File not found for {endpoint} in datasus_constants.DATASUS_ENDPOINT",
                    level="error",
                )
                raise FAIL(f"File not found for endpoint {endpoint}") from e

    log(f"Downloading file {file} from {datasus_constants.DATASUS_FTP_SERVER.value}")

    # Download file
    downloaded_file = download_ftp.run(
        host=host,
        directory=directory,
        file_name=file,
        output_path=download_path,
    )

    # Unzip file
    if downloaded_file is None:
        log(f"Failed to download file {file}", level="error")
        raise FAIL("Failed to download file")

    log("Unzipping file")
    if endpoint == "cbo":
        subprocess.run(
            f"unzip {downloaded_file} DBF/*CBO* -d {download_path}", shell=True, check=False
        )
        unziped_files = [
            f"{download_path}/{file}"
            for file in os.listdir(f"{download_path}/DBF")
            if file.endswith(".dbf")
        ]
    else:
        subprocess.run(f"unzip {downloaded_file} -d {download_path}", shell=True, check=False)
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
    if endpoint == "cbo":
        pass
    elif endpoint == "cnes":
        
        files = [fix_cnes_header(file) for file in files_path]
        log("Header fixed successfully")

        files = [add_flow_metadata(file_path=file, parse_date_from_filename=True) for file in files]
        log("Metadata added successfully")
        
        return files
    else:
        log(f"Endpoint {endpoint} not found", level="error")
        raise FAIL(f"Endpoint {endpoint} not found")
