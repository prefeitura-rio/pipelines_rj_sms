# -*- coding: utf-8 -*-
"""
Tasks for dump_ftp_cnes
"""

import os
import re
import shutil
import tempfile
from datetime import datetime

import pandas as pd
import prefect
import pytz
from prefect import task
from prefect.client import Client
from prefeitura_rio.pipelines_utils.bd import create_table_and_upload_to_gcs
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.dump_ftp_cnes.constants import constants
from pipelines.utils.tasks import (
    create_partitions,
    download_ftp,
    list_files_ftp,
    upload_to_datalake,
)


@task
def rename_current_flow_run_cnes(prefix: str, file: str):
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    return client.set_flow_run_name(flow_run_id, f"{prefix}{file}")


@task
def check_newest_file_version(host: str, user: str, password: str, directory: str, file_name: str):
    """
    Check the newest version of a file in a given FTP directory.

    Args:
        host (str): FTP server hostname.
        user (str): FTP server username.
        password (str): FTP server password.
        directory (str): FTP directory path.
        file_name (str): Base name of the file to check.

    Returns:
        str: The name of the newest version of the file.
    """
    file_name = constants.BASE_FILE.value
    files = list_files_ftp.run(host, user, password, directory)

    # filter a list of files that contains the base file name
    files = [file for file in files if file_name in file]

    # sort list descending
    files.sort(reverse=True)
    newest_file = files[0]

    # extract snapshot date from file
    snapshot_date = re.findall(r"\d{6}", newest_file)[0]
    snapshot_date = f"{snapshot_date[:4]}-{snapshot_date[-2:]}"

    log(f"Newest file: {newest_file}, snapshot_date: {snapshot_date}")
    return {"file": newest_file, "snapshot": snapshot_date}


@task
def check_file_to_download(
    download_newest: bool,
    file_to_download: None,
    partition_date: None,
    host: str,
    user: str,
    password: str,
    directory: str,
    file_name: str,
):
    """
    Check which file to download based on the given parameters.

    Args:
        download_newest (bool): Whether to download the newest file or not.
        file_to_download (None or str): The name of the file to download.
        partition_date (None or str): The partition date of the file to download.
        host (str): The FTP host to connect to.
        user (str): The FTP username.
        password (str): The FTP password.
        directory (str): The directory where the file is located.
        file_name (str): The name of the file to check.

    Returns:
        dict or str: If download_newest is True, returns the name of the newest file.
                    If download_newest is False, returns a dictionary with the file name
                    and partition date.
    Raises:
        ValueError: If download_newest is False and file_to_download or partition_date is
        not provided.
    """
    if download_newest:
        newest_file = check_newest_file_version.run(
            host=host,
            user=user,
            password=password,
            directory=directory,
            file_name=file_name,
        )
        return newest_file
    elif file_to_download is not None and partition_date is not None:
        return {"file": file_to_download, "snapshot": partition_date}
    else:
        raise ValueError(
            "If download_newest is False, file_to_download and partition_date must be provided"
        )


@task
def conform_csv_to_gcp(directory: str):
    """
    Conform CSV files in the given directory to be compatible with Google Cloud Storage.

    Args:
        directory (str): The directory containing the CSV files to be conformed.

    Returns:
        List[str]: A list of filepaths of the conformed CSV files.
    """
    # list all csv files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

    log(f"Conforming {len(csv_files)} files...")

    files_conform = []

    # iterate over each csv file
    for csv_file in csv_files:
        # construct the full file path
        filepath = os.path.join(directory, csv_file)

        # create a temporary file
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tf:
            # open the original file in read mode
            with open(filepath, "r", encoding="iso8859-1") as f:
                # read the first line
                first_line = f.readline()

                # modify the first line
                modified_first_line = first_line.replace("TO_CHAR(", "")
                modified_first_line = modified_first_line.replace(",'DD/MM/YYYY')", "")
                modified_first_line = modified_first_line.replace("A.DT", "DT")
                modified_first_line = modified_first_line.replace("'CO_CPF'", "CO_CPF")

                # write the modified first line to the temporary file
                tf.write(modified_first_line)

                # copy the rest of the lines from the original file to the temporary file
                shutil.copyfileobj(f, tf)

        # replace the original file with the temporary file
        shutil.move(tf.name, filepath)
        files_conform.append(filepath)

    log("Conform done.")

    return files_conform


@task
def upload_multiple_tables_to_datalake(path_files: str, dataset_id: str, dump_mode: str):
    """
    Uploads multiple tables to datalake.

    Args:
        path_files (str): The path to the files to be uploaded.
        dataset_id (str): The ID of the dataset to upload the files to.
        dump_mode (str): The dump mode to use for the upload.

    Returns:
        None
    """
    for n, file in enumerate(path_files):
        log(f"Uploading {n+1}/{len(path_files)} files to datalake...")

        # retrieve file name from path
        file_name = os.path.basename(file)

        # replace 6 digits numbers from string
        table_id = re.sub(r"\d{6}", "", file_name)
        table_id = table_id.replace(".csv", "")

        upload_to_datalake.run(
            input_path=file,
            dataset_id=dataset_id,
            table_id=table_id,
            if_exists="replace",
            csv_delimiter=";",
            if_storage_data_exists="replace",
            biglake_table=True,
            dump_mode=dump_mode,
        )


@task
def add_multiple_date_column(directory: str, sep=";", snapshot_date=None):
    """
    Adds date metadata columns to all CSV files in a given directory.

    Args:
        directory (str): The directory containing the CSV files.
        sep (str, optional): The delimiter used in the CSV files. Defaults to ";".
        snapshot_date (str, optional): The date of the snapshot. Defaults to None.
    """
    tz = pytz.timezone("Brazil/East")
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    # list all csv files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

    # iterate over each csv file
    for n, csv_file in enumerate(csv_files):
        log(f"Adding date metadata to {n+1}/{len(csv_files)} files ...")
        # construct the full file path

        filepath = os.path.join(directory, csv_file)

        df = pd.read_csv(filepath, sep=sep, keep_default_na=False, dtype="str")
        df["_data_carga"] = now
        df["_data_snapshot"] = snapshot_date

        df.to_csv(filepath, index=False, sep=sep, encoding="utf-8")
        log(f"Column added to {filepath}")


@task
def convert_csv_to_parquet(directory: str, sep=";"):
    """
    Convert CSV files in the specified directory to Parquet format.

    Args:
        directory (str): The directory path where the CSV files are located.
        sep (str, optional): The delimiter used in the CSV files. Defaults to ";".

    Returns:
        list: A list of file paths for the converted Parquet files.
    """
    # list all csv files in the directory
    csv_files = [f for f in os.listdir(directory) if f.endswith(".csv")]

    # iterate over each csv file
    for n, csv_file in enumerate(csv_files):
        log(f"Creating parquet file: {n+1}/{len(csv_files)} files ...")
        # construct the full file path

        filepath = os.path.join(directory, csv_file)

        df = pd.read_csv(filepath, sep=sep, keep_default_na=False, dtype="str")
        df.to_parquet(filepath.replace(".csv", ".parquet"), index=False)

        log(f"Parquet file created {filepath.replace('.csv', '.parquet')}")

    parquet_files = [
        os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(".parquet")
    ]
    return parquet_files


@task
def download_ftp_cnes(host, user, password, directory, file_name, output_path):
    """
    Downloads a file from an FTP server.

    Args:
        host (str): The FTP server hostname.
        user (str): The FTP server username.
        password (str): The FTP server password.
        directory (str): The directory where the file is located.
        file_name (str): The name of the file to download.
        output_path (str): The local path where the file will be saved.

    Returns:
        str: The local path where the downloaded file was saved.
    """
    return download_ftp.run(
        host=host,
        user=user,
        password=password,
        directory=directory,
        file_name=file_name,
        output_path=output_path,
    )


@task
def create_partitions_and_upload_multiple_tables_to_datalake(
    path_files: str,
    partition_folder: str,
    partition_date: str,
    dataset_id: str,
    dump_mode: str,
):
    """
    Uploads multiple tables to datalake.

    Args:
        path_files (str): The path to the files to be uploaded.
        dataset_id (str): The ID of the dataset to upload the files to.
        dump_mode (str): The dump mode to use for the upload.

    Returns:
        None
    """

    for n, file in enumerate(path_files):
        log(f"Uploading {n+1}/{len(path_files)} files to datalake...")

        # retrieve file name from path
        file_name = os.path.basename(file)

        # replace 6 digits numbers from string
        table_id = re.sub(r"\d{6}", "", file_name)
        table_id = table_id.replace(".parquet", "")

        table_partition_folder = os.path.join(partition_folder, table_id)

        create_partitions.run(
            data_path=file,
            partition_directory=table_partition_folder,
            level="month",
            partition_date=partition_date,
        )

        create_table_and_upload_to_gcs(
            data_path=table_partition_folder,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode=dump_mode,
            biglake_table=True,
            source_format="parquet",
        )
