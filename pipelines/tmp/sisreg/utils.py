# -*- coding: utf-8 -*-
# pylint: disable=line-too-long, C0114
# flake8: noqa: E501

import glob
import os

from google.cloud import storage
from prefeitura_rio.pipelines_utils.logging import log


def get_first_csv(folder_path):
    """
    Returns the path of the first CSV file found in the specified folder.

    Args:
        folder_path (str): The path of the folder to search for CSV files.

    Returns:
        str or None: The path of the first CSV file found, or None if no CSV files are found.
    """
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if csv_files:
        return csv_files[0]
    else:
        return None


def upload_to_gcs(bucket_name, local_file_path):
    """Uploads a file to Google Cloud Storage.

    Args:
        bucket_name (str): Name of the bucket.
        local_file_path (str): Path to the local file to be uploaded.
    """

    # Initialize a client
    try:
        file_name = os.path.basename(local_file_path)

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        log("Uploading file to GCS...")
        blob.upload_from_filename(local_file_path, timeout=1200)

        log(f"File uploaded to {bucket_name}/{file_name}.")

    except Exception as e:
        log(f"An error occurred: {e}", level="error")
        raise e
