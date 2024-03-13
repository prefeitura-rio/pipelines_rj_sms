# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
General utilities for SMS pipelines.
"""

import json
import os
import re
import shutil
import sys
import zipfile
from datetime import date, datetime, timedelta
from ftplib import FTP
from io import FileIO, StringIO
from pathlib import Path

import basedosdados as bd
import google.auth.transport.requests
import google.oauth2.id_token
import gspread
import pandas as pd
import prefect
import pytz
import requests
from azure.storage.blob import BlobServiceClient
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from prefect import task
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Failed
from prefeitura_rio.pipelines_utils.env import getenv_or_action
from prefeitura_rio.pipelines_utils.infisical import get_infisical_client, get_secret
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.utils.data_cleaning import remove_columns_accents
from pipelines.utils.infisical import get_credentials_from_env, inject_bd_credentials


@task
def get_secret_key(secret_path: str, secret_name: str, environment: str) -> str:
    """
    Retrieves the secret key from the specified secret path and name.

    Args:
        secret_path (str): The path to the secret.
        secret_name (str): The name of the secret.

    Returns:
        str: The secret key.

    """
    secret = get_secret(secret_name=secret_name, path=secret_path, environment=environment)
    return secret[secret_name]


@task
def get_infisical_user_password(path: str, environment: str = "dev") -> tuple:
    """
    Retrieves the Infisical username and password from the specified secret path.

    Args:
        environment (str, optional): The Infisical environment to retrieve the credentials from. Defaults to 'dev'.
        path (str): The path to the secret.

    Returns:
        tuple: A tuple containing the username and password.
    """
    username = get_secret(secret_name="USERNAME", path=path, environment=environment)
    password = get_secret(secret_name="PASSWORD", path=path, environment=environment)

    return {"username": username["USERNAME"], "password": password["PASSWORD"]}


@task(max_retries=3, retry_delay=timedelta(seconds=60))
def inject_gcp_credentials(environment: str = "dev") -> None:
    """
    Injects GCP credentials into the specified environment.

    Args:
        environment (str, optional): The environment to inject the credentials into. Defaults to 'dev'.
    """
    inject_bd_credentials(environment=environment)


@task
def list_all_secrets_name(
    environment: str = "dev",
    path: str = "/",
) -> None:
    "List all secrets in a path"
    token = getenv_or_action("INFISICAL_TOKEN", default=None)
    log(f"""Token: {token}""")

    client = get_infisical_client()
    secrets = client.get_all_secrets(environment=environment, path=path)

    secret_names = []
    for secret in secrets:
        secret_names.append(secret.secret_name)

    log(f"""Secrets in path: {", ".join(secret_names)}""")


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

    except Exception as e:  # pylint: disable=W0703
        sys.exit(f"Failed to create folders: {e}")


@task
def download_from_api(
    url: str,
    file_folder: str,
    file_name: str,
    params=None,
    credentials=None,
    auth_method="bearer",
    add_load_date_to_filename=False,
    load_date=None,
):
    """
    Downloads data from an API and saves it to a local file.

    Args:
        url (str): The URL of the API endpoint.
        file_folder (str): The folder where the downloaded file will be saved.
        file_name (str): The name of the downloaded file.
        params (dict, optional): Additional parameters to be included in the API request. Defaults to None.
        credentials (str or tuple, optional): The credentials to be used for authentication. Defaults to None.
        auth_method (str, optional): The authentication method to be used. Valid values are "bearer" and "basic". Defaults to "bearer".
        add_load_date_to_filename (bool, optional): Whether to add the load date to the filename. Defaults to False.
        load_date (str, optional): The load date to be added to the filename. Defaults to None.

    Returns:
        str: The file path of the downloaded file.

    Raises:
        ValueError: If the API call fails.
    """  # noqa: E501

    api_data = load_from_api.run(
        url=url,
        params=params,
        credentials=credentials,
        auth_method=auth_method,
    )

    # Save the API data to a local file
    if add_load_date_to_filename:
        if load_date is None:
            destination_file_path = f"{file_folder}/{file_name}_{str(date.today())}.json"
        else:
            destination_file_path = f"{file_folder}/{file_name}_{load_date}.json"
    else:
        destination_file_path = f"{file_folder}/{file_name}.json"

    with open(destination_file_path, "w", encoding="utf-8") as file:
        file.write(str(api_data))

    log(f"API data downloaded to {destination_file_path}")

    return destination_file_path


@task(max_retries=3, retry_delay=timedelta(seconds=30))
def load_from_api(url: str, params=None, credentials=None, auth_method="bearer") -> dict:
    """
    Loads data from an API endpoint.

    Args:
        url (str): The URL of the API endpoint.
        params (dict, optional): The query parameters to be included in the request. Defaults to None.
        credentials (str or tuple, optional): The authentication credentials. Defaults to None.
        auth_method (str, optional): The authentication method to be used. Defaults to "bearer".

    Returns:
        dict: The JSON response from the API.

    Raises:
        Exception: If the API request fails.
    """
    if auth_method == "bearer":
        headers = {"Authorization": f"Bearer {credentials}"}
        response = requests.get(url, headers=headers, params=params, timeout=180)
    elif auth_method == "basic":
        response = requests.get(url, auth=credentials, params=params, timeout=180)
    else:
        response = requests.get(url, params=params, timeout=180)

    if response.status_code == 200:
        return response.json()
    else:
        raise ValueError(f"API call failed, error: {response.status_code} - {response.reason}")


@task
def download_azure_blob(
    container_name: str,
    blob_path: str,
    file_folder: str,
    file_name: str,
    credentials=None,
    add_load_date_to_filename=False,
    load_date=None,
):
    """
    Downloads data from Azure Blob Storag and saves it to a local file.

    Args:
        container_name (str): The name of the container in the Azure Blob Storage account.
        blob_path (str): The path to the blob in the container.
        file_folder (str): The folder where the downloaded file will be saved.
        file_name (str): The name of the downloaded file.
        params (dict, optional): Additional parameters to include in the API request.
        credentials (str or tuple, optional): The credentials to be used for authentication. Defaults to None.
        add_load_date_to_filename (bool, optional): Whether to add the current date to the filename.
        load_date (str, optional): The specific date to add to the filename.

    Returns:
        str: The path of the downloaded file.
    """  # noqa: E501

    # Download data from Blob Storage
    log(f"Downloading data from Azure Blob Storage: {blob_path}")
    blob_service_client = BlobServiceClient(
        account_url="https://datalaketpcgen2.blob.core.windows.net/",
        credential=credentials,
    )
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)

    # Save the API data to a local file
    if add_load_date_to_filename:
        if load_date is None:
            destination_file_path = f"{file_folder}/{file_name}_{str(date.today())}.csv"
        else:
            destination_file_path = f"{file_folder}/{file_name}_{load_date}.csv"
    else:
        destination_file_path = f"{file_folder}/{file_name}.csv"

    with open(destination_file_path, "wb") as file:
        blob_data = blob_client.download_blob()
        blob_data.readinto(file)

    log(f"Blob downloaded to '{destination_file_path}")

    return destination_file_path


@task(max_retries=3, retry_delay=timedelta(seconds=60), timeout=timedelta(minutes=5))
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


@task()
def download_from_url(  # pylint: disable=too-many-arguments
    url: str,
    file_path,
    file_name,
    url_type: str = "google_sheet",
    gsheets_sheet_order: int = 0,
    gsheets_sheet_name: str = None,
    gsheets_sheet_range: str = None,
    csv_delimiter: str = ";",
) -> None:
    """
    Downloads a file from a URL and saves it to a local file.
    Try to do it without using lots of RAM.
    It is not optimized for Google Sheets downloads.

    Args:
        url: URL to download from.
        file_name: Name of the file to save to.
        url_type: Type or URL that is being passed.
            `google_sheet`-> Google Sheet URL.
        gsheets_sheet_order: Worksheet index, in the case you want to select it by index. \
            Worksheet indexes start from zero.
        gsheets_sheet_name: Worksheet name, in the case you want to select it by name.
        gsheets_sheet_range: Range in selected worksheet to get data from. Defaults to entire \
            worksheet.

    Returns:
        None.
    """
    if not ".csv" in file_name:
        file_name = file_name + ".csv"
    filepath = os.path.join(file_path, file_name)

    if url_type == "google_sheet":
        if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
            raise ValueError(
                "GOOGLE_APPLICATION_CREDENTIALS environment variable must be set to use this task."
            )

        credentials = get_credentials_from_env(
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ]
        )

        url_prefix = "https://docs.google.com/spreadsheets/d/"

        if not url.startswith(url_prefix):
            raise ValueError(
                "URL must start with https://docs.google.com/spreadsheets/d/" f"Invalid URL: {url}"
            )

        log(">>>>> URL is a Google Sheets URL, downloading directly")

        gspread_client = gspread.authorize(credentials)

        sheet = gspread_client.open_by_url(url)
        if gsheets_sheet_name:
            worksheet = sheet.worksheet(gsheets_sheet_name)
        else:
            worksheet = sheet.get_worksheet(gsheets_sheet_order)
        if gsheets_sheet_range:  # if range is informed, get range from worksheet
            dataframe = pd.DataFrame(worksheet.batch_get((gsheets_sheet_range,))[0])
        else:
            dataframe = pd.DataFrame(worksheet.get_values())
        new_header = dataframe.iloc[0]  # grab the first row for the header
        dataframe = dataframe[1:]  # take the data less the header row
        dataframe.columns = new_header  # set the header row as the df header
        log(f">>>>> Dataframe shape: {dataframe.shape}")
        log(f">>>>> Dataframe columns: {dataframe.columns}")
        dataframe.columns = remove_columns_accents(dataframe)
        log(f">>>>> Dataframe columns after treatment: {dataframe.columns}")

        dataframe.to_csv(filepath, index=False, sep=csv_delimiter, encoding="utf-8")
    else:
        raise ValueError("Invalid URL type. Please set values to `url_type` parameter")


@task(
    max_retries=2,
    retry_delay=timedelta(seconds=120),
)
def cloud_function_request(
    url: str,
    credential: None,
    request_type: str = "GET",
    body_params: list = None,
    query_params: list = None,
    env: str = "dev",
):
    """
    Sends a request to an endpoint trough a cloud function.
    This method is used when the endpoint is only accessible through a fixed IP.

    Args:
        url (str): The URL of the endpoint.
        request_type (str, optional): The type of the request (e.g., GET, POST). Defaults to "GET".
        body_params (list, optional): The body parameters of the request. Defaults to None.
        query_params (list, optional): The query parameters of the request. Defaults to None.
        env (str, optional): The environment of the cloud function (e.g., staging, prod). Defaults to "staging".
        credential (None): The credential for the request. Defaults to None.

    Returns:
        requests.Response: The response from the cloud function.
    """  # noqa: E501

    # Get Prefect Logger
    logger = prefect.context.get("logger")

    if env == "prod":
        cloud_function_url = "https://us-central1-rj-sms.cloudfunctions.net/vitacare"
    elif env == "dev":
        cloud_function_url = "https://us-central1-rj-sms-dev.cloudfunctions.net/vitacare"
    else:
        raise ValueError("env must be 'prod' or 'dev'")

    # TOKEN = os.environ.get("GOOGLE_TOKEN")
    request = google.auth.transport.requests.Request()
    TOKEN = google.oauth2.id_token.fetch_id_token(request, cloud_function_url)

    payload = json.dumps(
        {
            "url": url,
            "request_type": request_type,
            "body_params": json.dumps(body_params),
            "query_params": query_params,
            "credential": credential,
        }
    )
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"}

    try:
        response = requests.request("POST", cloud_function_url, headers=headers, data=payload)

        if response.status_code == 200:
            logger.info("[Cloud Function] Request was Successful")

            payload = response.json()

            if payload["status_code"] != 200:
                message = f"[Target Endpoint] Request failed: {payload['status_code']} - {payload['body']}"
                logger.info(message)
                raise ENDRUN(state=Failed(message))
            else:
                logger.info("[Target Endpoint] Request was successful")

                return payload

        else:
            message = f"[Cloud Function] Request failed: {response.status_code} - {response.reason}"
            logger.info(message)
            raise ENDRUN(state=Failed(message))

    except Exception as e:
        raise ENDRUN(
            state=Failed(f"[Cloud Function] Request failed with unknown error: {e}")
        ) from e


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
def save_to_file(data, file_folder, file_name, add_load_date_to_filename, load_date):
    """
    Save the API data to a local file.

    Args:
        data: The API data to be saved.
        file_folder: The folder where the file will be saved.
        file_name: The name of the file.
        add_load_date_to_filename: A boolean indicating whether to add the load date to the filename.
        load_date: The load date to be added to the filename.

    Returns:
        The path of the saved file.
    """  # noqa: E501
    if add_load_date_to_filename:
        if load_date is None:
            destination_file_path = f"{file_folder}/{file_name}_{str(date.today())}.json"
        else:
            destination_file_path = f"{file_folder}/{file_name}_{load_date}.json"
    else:
        destination_file_path = f"{file_folder}/{file_name}.json"

    with open(destination_file_path, "w", encoding="utf-8") as file:
        file.write(str(data))

    log(f"API data downloaded to {destination_file_path}")

    return destination_file_path


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
def add_load_date_column(input_path: str, sep=";", load_date=None):
    """
    Adds a new column '_data_carga' to a CSV file located at input_path with the current date
    or a specified load date.

    Args:
        input_path (str): The path to the input CSV file.
        sep (str, optional): The delimiter used in the CSV file. Defaults to ";".
        load_date (str, optional): The load date to be used in the '_data_carga' column. If None,
        the current date is used. Defaults to None.

    Returns:
        str: The path to the input CSV file.
    """
    tz = pytz.timezone("Brazil/East")
    now = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    df = pd.read_csv(input_path, sep=sep, keep_default_na=False, dtype="str")

    if load_date is None:
        df["_data_carga"] = now
    else:
        df["_data_carga"] = load_date

    df.to_csv(input_path, index=False, sep=sep, encoding="utf-8")
    log(f"Column added to {input_path}")
    return input_path


@task
def from_json_to_csv(input_path, sep=";"):
    """
    Converts a JSON file to a CSV file.

    Args:
        input_path (str): The path to the input JSON file.
        sep (str, optional): The separator to use in the output CSV file. Defaults to ";".

    Returns:
        str: The path to the output CSV file, or None if an error occurred.
    """
    try:
        with open(input_path, "r", encoding="utf-8") as file:
            json_data = file.read()
            data = eval(json_data)  # Convert JSON string to Python dictionary
            output_path = input_path.replace(".json", ".csv")
            # Assuming the JSON structure is a list of dictionaries
            df = pd.DataFrame(data, dtype="str")
            df.to_csv(output_path, index=False, sep=sep, encoding="utf-8")

            log("JSON converted to CSV")
            return output_path

    except Exception as e:  # pylint: disable=W0703
        log(f"An error occurred: {e}", level="error")
        return None


@task
def create_partitions(data_path: str, partition_directory: str, level="day", partition_date=None):
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
            mes_particao = parsed_date.strftime("%m")

            output_directory = f"{partition_directory}/ano_particao={int(ano_particao)}/mes_particao={mes_particao}"  # noqa: E501

        else:
            raise ValueError("Partition level must be day or month")

        # Create partition directory
        if not os.path.exists(output_directory):
            os.makedirs(output_directory, exist_ok=False)

        # Copy file(s) to partition directory
        shutil.copy(file_name, output_directory)

    log("Partitions created successfully")


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

    try:
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
                log(f"TABLE ALREADY EXISTS APPENDING DATA TO STORAGE: {dataset_id}.{table_id}")

                tb.append(filepath=input_path, if_exists=if_exists)
            elif dump_mode == "overwrite":
                log(
                    "MODE OVERWRITE: Table ALREADY EXISTS, DELETING OLD DATA!\n"
                    f"{storage_path}\n"
                    f"{storage_path_link}"
                )  # pylint: disable=C0301
                st.delete_table(mode="staging", bucket_name=st.bucket_name, not_found_ok=True)
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

    except Exception as e:  # pylint: disable=W0703
        log(f"An error occurred: {e}", level="error")


@task(max_retries=3, retry_delay=timedelta(seconds=90))
def load_file_from_gcs_bucket(bucket_name, file_name, file_type="csv", csv_sep=","):
    """
    Load a file from a Google Cloud Storage bucket. The GCS project is infered
        from the environment variables related to Goocle Cloud.

    Args:
        bucket_name (str): The name of the GCS bucket.
        file_name (str): The name of the file to be loaded.
        file_type (str, optional): The type of the file. Defaults to "csv".

    Raises:
        NotImplementedError: If the file type is not implemented.

    Returns:
        pd.DataFrame: The loaded file as a DataFrame.
    """
    client = storage.Client()

    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)

    data = blob.download_as_string()

    if file_type == "csv":
        df = pd.read_csv(StringIO(data.decode("utf-8")), dtype=str, sep=csv_sep)
    else:
        raise NotImplementedError(f"File type {file_type} not implemented")

    return df


@task
def load_file_from_bigquery(project_name: str, dataset_name: str, table_name: str):
    """
    Load data from BigQuery table into a pandas DataFrame.

    Args:
        project_name (str): The name of the BigQuery project.
        dataset_name (str): The name of the BigQuery dataset.
        table_name (str): The name of the BigQuery table.

    Returns:
        pandas.DataFrame: The loaded data from the BigQuery table.
    """
    client = bigquery.Client()

    dataset_ref = bigquery.DatasetReference(project_name, dataset_name)
    table_ref = dataset_ref.table(table_name)
    table = client.get_table(table_ref)

    df = client.list_rows(table).to_dataframe()

    return df
