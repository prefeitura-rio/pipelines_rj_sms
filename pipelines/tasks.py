"""
General utilities for SMS pipelines.
"""
import os
import re
import sys
import shutil
import zipfile
from ftplib import FTP
from pathlib import Path
from prefect import task
import basedosdados as bd
from datetime import datetime
from prefeitura_rio.pipelines_utils.logging import log

@task
def create_folders():
    """
    Creates two directories, './data/raw' and './data/partition_directory', and returns their paths.

    Returns:
        dict: A dictionary with the paths of the created directories.
            - "data": "./data/raw"
            - "partition_directory": "./data/partition_directory"
    """
    try:
        path_raw_data = os.path.join(os.getcwd(), "data", "raw")
        path_partionared_data = os.path.join(os.getcwd(), "data", "partition_directory")

        if os.path.exists(path_raw_data):
            shutil.rmtree(path_raw_data, ignore_errors=False)
        os.makedirs(path_raw_data)

        if os.path.exists(path_partionared_data):
            shutil.rmtree(path_partionared_data, ignore_errors=False)
        os.makedirs(path_partionared_data)

        folders = {
            "raw": path_raw_data,
            "partition_directory": path_partionared_data,
        }

        log(f"Folders created: {folders}")
        return folders

    except Exception as e:
        sys.exit(f"Failed to create folders: {e}")

@task
def unzip_file(file_path: str, output_path: str):
    """
    Unzips a file to the specified output path.

    Args:
        file_path (str): The path to the file to be unzipped.
        output_path (str): The path to the output directory.

    Returns:
        str: The path to the unzipped file.
    """
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(output_path)

    log(f"File unzipped to {output_path}")

    return output_path

@task
def list_files_ftp(host, user, password, directory):
    """
    Lists all files in a given directory on a remote FTP server.

    Args:
        host (str): The hostname or IP address of the FTP server.
        user (str): The username to use for authentication.
        password (str): The password to use for authentication.
        directory (str): The directory to list files from.

    Returns:
        list: A list of filenames in the specified directory.
    """
    ftp = FTP(host)
    ftp.login(user, password)
    ftp.cwd(directory)

    files = ftp.nlst()

    ftp.quit()

    return files


@task
def upload_to_datalake(
    input_path: str,
    dataset_id: str,
    table_id: str,
    if_exists: str = "replace",
    csv_delimiter: str = ";",
    if_storage_data_exists: str = "replace",
    biglake_table: bool = True,
    dataset_is_public: bool = False,
    dump_mode: str = "append",
):
    """
    Uploads data from a file to a BigQuery table in a specified dataset.

    Args:
        input_path (str): The path to the file containing the data to be uploaded.
        dataset_id (str): The ID of the dataset where the table is located.
        table_id (str): The ID of the table where the data will be uploaded.
        if_exists (str, optional): Specifies what to do if the table already exists.
            Defaults to "replace".
        csv_delimiter (str, optional): The delimiter used in the CSV file. Defaults to ";".
        if_storage_data_exists (str, optional): Specifies what to do if the storage data
            already exists. Defaults to "replace".
        biglake_table (bool, optional): Specifies whether the table is a BigLake table.
            Defaults to True.
    """
    tb = bd.Table(dataset_id=dataset_id, table_id=table_id)
    table_staging = f"{tb.table_full_name['staging']}"
    st = bd.Storage(dataset_id=dataset_id, table_id=table_id)
    storage_path = f"{st.bucket_name}.staging.{dataset_id}.{table_id}"
    storage_path_link = (
        f"https://console.cloud.google.com/storage/browser/{st.bucket_name}"
        f"/staging/{dataset_id}/{table_id}"
    )

    table_exists = tb.table_exists(mode="staging")

    if not table_exists:
        log(f"CREATING TABLE: {dataset_id}.{table_id}")
        tb.create(
            path=input_path,
            csv_delimiter=csv_delimiter,
            if_storage_data_exists=if_storage_data_exists,
            biglake_table=biglake_table,
            dataset_is_public=dataset_is_public,
        )
    else:
        if dump_mode == "append":
            log(
                f"TABLE ALREADY EXISTS APPENDING DATA TO STORAGE: {dataset_id}.{table_id}"
            )

            tb.append(filepath=input_path, if_exists=if_exists)
        elif dump_mode == "overwrite":
            log(
                "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            st.delete_table(
                mode="staging", bucket_name=st.bucket_name, not_found_ok=True
            )
            log(
                "MODE OVERWRITE: Sucessfully DELETED OLD DATA from Storage:\n"
                f"{storage_path}\n"
                f"{storage_path_link}"
            )  # pylint: disable=C0301
            tb.delete(mode="all")
            log(
                "MODE OVERWRITE: Sucessfully DELETED TABLE:\n" f"{table_staging}\n"
            )  # pylint: disable=C0301

            tb.create(
                path=input_path,
                csv_delimiter=csv_delimiter,
                if_storage_data_exists=if_storage_data_exists,
                biglake_table=biglake_table,
                dataset_is_public=dataset_is_public,
            )
    log("Data uploaded to BigQuery")


@task
def download_ftp(
    host: str,
    user: str,
    password: str,
    directory: str,
    file_name: str,
    output_path: str,
):
    """
    Downloads a file from an FTP server and saves it to the specified output path.

    Args:
        host (str): The FTP server hostname.
        user (str): The FTP server username.
        password (str): The FTP server password.
        directory (str): The directory on the FTP server where the file is located.
        file_name (str): The name of the file to download.
        output_path (str): The local path where the downloaded file should be saved.

    Returns:
        str: The local path where the downloaded file was saved.
    """

    file_path = f"{directory}/{file_name}"
    output_path = output_path + "/" + file_name
    log(output_path)
    # Connect to the FTP server
    ftp = FTP(host)
    ftp.login(user, password)

    # Get the size of the file
    ftp.voidcmd("TYPE I")
    total_size = ftp.size(file_path)

    # Create a callback function to be called when each block is read
    def callback(block):
        nonlocal downloaded_size
        downloaded_size += len(block)
        percent_complete = (downloaded_size / total_size) * 100
        if percent_complete // 5 > (downloaded_size - len(block)) // (total_size / 20):
            log(f"Download is {percent_complete:.0f}% complete")
        f.write(block)

    # Initialize the downloaded size
    downloaded_size = 0

    # Download the file
    with open(output_path, "wb") as f:
        ftp.retrbinary(f"RETR {file_path}", callback)

    # Close the connection
    ftp.quit()

    return output_path


@task
def create_partitions(
    data_path: str, partition_directory: str, level="day", partition_date=None
):
    """
    Creates partitions for each file in the given data path and saves them in the
    partition directory.

    Args:
        data_path (str): The path to the data directory.
        partition_directory (str): The path to the partition directory.
        level (str): The level of partitioning. Can be "day" or "month". Defaults to "day".

    Raises:
        FileExistsError: If the partition directory already exists.
        ValueError: If the partition level is not "day" or "month".

    Returns:
        None
    """

    # check if data_path is a directory or a file
    if os.path.isdir(data_path):
        data_path = Path(data_path)
        files = data_path.glob("*.csv")
    else:
        files = [data_path]
    #
    # Create partition directories for each file
    for file_name in files:
        if level == "day":
            if partition_date is None:
                try:
                    date_str = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name)).group()
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    log(
                        "Filename must contain a date in the format YYYY-MM-DD to match partition level",  # noqa: E501
                        level="error",
                    )  # noqa: E501")
            else:
                try:
                    parsed_date = datetime.strptime(partition_date, "%Y-%m-%d")
                except ValueError:
                    log(
                        "Partition date must be in the format YYYY-MM-DD to match partition level",  # noqa: E501
                        level="error",
                    )

            ano_particao = parsed_date.strftime("%Y")
            mes_particao = parsed_date.strftime("%m")
            data_particao = parsed_date.strftime("%Y-%m-%d")

            output_directory = f"{partition_directory}/ano_particao={int(ano_particao)}/mes_particao={int(mes_particao)}/data_particao={data_particao}"  # noqa: E501

        elif level == "month":
            if partition_date is None:
                try:
                    date_str = re.search(r"\d{4}-\d{2}", str(file_name)).group()
                    parsed_date = datetime.strptime(date_str, "%Y-%m")
                except ValueError:
                    log(
                        "File must contain a date in the format YYYY-MM", level="error"
                    )  # noqa: E501")
            else:
                try:
                    parsed_date = datetime.strptime(partition_date, "%Y-%m")
                except ValueError:
                    log(
                        "Partition date must match be in the format YYYY-MM to match partitio level",  # noqa: E501
                        level="error",
                    )

            ano_particao = parsed_date.strftime("%Y")
            mes_particao = parsed_date.strftime("%Y-%m")

            output_directory = f"{partition_directory}/ano_particao={int(ano_particao)}/mes_particao={mes_particao}"  # noqa: E501

        else:
            raise ValueError("Partition level must be day or month")

        # Create partition directory
        if not os.path.exists(output_directory):
            os.makedirs(output_directory, exist_ok=False)

        # Copy file(s) to partition directory
        shutil.copy(file_name, output_directory)

    log("Partitions created successfully")

