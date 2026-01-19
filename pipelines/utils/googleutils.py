# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0301
# flake8: noqa: E501

"""
Functions to interact with Google Cloud Storage and BigQuery.
"""

import os

import pandas as pd
from google.cloud import bigquery, storage


def generate_bigquery_schema(df: pd.DataFrame) -> list[bigquery.SchemaField]:
    """
    Generates a BigQuery schema based on the provided DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame for which the BigQuery schema needs to be generated.

    Returns:
        list[bigquery.SchemaField]: The generated BigQuery schema as a list of SchemaField objects.
    """
    TYPE_MAPPING = {
        "i": "INTEGER",
        "u": "NUMERIC",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "TIMESTAMP",
    }
    schema = []
    for column, dtype in df.dtypes.items():
        val = df[column].iloc[0]
        mode = "REPEATED" if isinstance(val, list) else "NULLABLE"

        if isinstance(val, dict) or (mode == "REPEATED" and isinstance(val[0], dict)):
            fields = generate_bigquery_schema(pd.json_normalize(val))
        else:
            fields = ()

        _type = "RECORD" if fields else TYPE_MAPPING.get(dtype.kind)
        schema.append(
            bigquery.SchemaField(
                name=column,
                field_type=_type,
                mode=mode,
                fields=fields,
            )
        )
    return schema


def download_from_cloud_storage(path: str, bucket_name: str, blob_prefix: str = None):
    """
    Downloads files from Google Cloud Storage to the specified local path.

    Args:
        path (str): The local path where the files will be downloaded to.
        bucket_name (str): The name of the Google Cloud Storage bucket.
        blob_prefix (str, optional): The prefix of the blobs to download. Defaults to None.
    """

    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    downloaded_files = []

    if not os.path.exists(path):
        os.makedirs(path)

    blobs = bucket.list_blobs(prefix=blob_prefix)

    for blob in blobs:

        destination_file_name = os.path.join(path, blob.name)

        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)

        try:
            blob.download_to_filename(destination_file_name)

            downloaded_files.append(destination_file_name)

        except IsADirectoryError:
            pass

    return downloaded_files


def upload_to_cloud_storage(
    path: str,
    bucket_name: str,
    blob_prefix: str = None,
    if_exist: str = "replace",
):
    """
    Uploads a file or a folder to Google Cloud Storage.

    Args:
        path (str): The path to the file or folder that needs to be uploaded.
        bucket_name (str): The name of the bucket in Google Cloud Storage.
        blob_prefix (str, optional): The prefix of the blob to upload. Defaults to None.
        if_exist (str, optional): The action to take if the blob already exists. Defaults to "replace".
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    if if_exist not in ["replace", "skip"]:
        raise ValueError("Invalid if_exist value. Please use 'replace' or 'skip'.")

    if os.path.isfile(path):
        # Upload a single file
        blob_name = os.path.basename(path)
        if blob_prefix:
            blob_name = f"{blob_prefix}/{blob_name}"
        blob = bucket.blob(blob_name)

        if if_exist == "skip" and blob.exists():
            return

        blob.upload_from_filename(path)
    elif os.path.isdir(path):
        # Upload a folder
        for root, _, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                blob_name = os.path.relpath(file_path, path)
                if blob_prefix:
                    blob_name = f"{blob_prefix}/{blob_name}"
                blob = bucket.blob(blob_name)

                if if_exist == "skip" and blob.exists():
                    continue

                blob.upload_from_filename(file_path)
    else:
        raise ValueError("Invalid path provided.")


def clear_bucket(bucket_name: str):
    """
    Clears a Google Cloud Storage bucket by deleting all the blobs.

    Args:
        bucket_name (str): The name of the bucket in Google Cloud Storage.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs()

    for blob in blobs:
        blob.delete()
