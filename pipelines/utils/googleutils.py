# -*- coding: utf-8 -*-
# pylint: disable=C0103, C0301
# flake8: noqa: E501
"""
Functions to interact with Google Cloud services.
"""

import os

import subprocess
import pandas as pd
from google.cloud import bigquery, storage
from prefeitura_rio.pipelines_utils.logging import log


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

    if not os.path.exists(path):
        os.makedirs(path)

    blobs = bucket.list_blobs(prefix=blob_prefix)
    for blob in blobs:

        destination_file_name = os.path.join(path, blob.name)

        os.makedirs(os.path.dirname(destination_file_name), exist_ok=True)

        blob.download_to_filename(destination_file_name)


def upload_to_cloud_storage(path: str, bucket_name: str):
    """
    Uploads a file or a folder to Google Cloud Storage.

    Args:
        path (str): The path to the file or folder that needs to be uploaded.
        bucket_name (str): The name of the bucket in Google Cloud Storage.
    """
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)

    if os.path.isfile(path):
        # Upload a single file
        blob = bucket.blob(os.path.basename(path))
        blob.upload_from_filename(path)
    elif os.path.isdir(path):
        # Upload a folder
        for root, _, files in os.walk(path):
            for file in files:
                file_path = os.path.join(root, file)
                blob = bucket.blob(os.path.relpath(file_path, path))
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


def tag_bigquery_table(
    project_id: str, dataset_id: str, table_id: str, tag_key: str, tag_value: str
):
    '''
    Tag a BigQuery table with a specified key-value pair.

    Args:
        project_id (str): The ID of the project containing the BigQuery table.
        dataset_id (str): The ID of the dataset containing the BigQuery table.
        table_id (str): The ID of the BigQuery table to tag.
        tag_key (str): The key of the tag to add.
        tag_value (str): The value of the tag to add.

    Returns:
        CompletedProcess: Result of the subprocess run.

    Raises:
        CalledProcessError: If an error occurs during the subprocess run.
    '''

    command = [
        "bq",
        "update",
        f"--add_tags={project_id}/{tag_key}:{tag_value}",
        f"{project_id}:{dataset_id}.{table_id}",
    ]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        log(f"{result.stdout}Value: {project_id}/{tag_key}: {tag_value}", level="info")
    except subprocess.CalledProcessError as e:
        log(f"Error adding tag: {e.stderr}", level="error")
        raise


def label_bigquery_table(
    project_id: str, dataset_id: str, table_id: str, label_key: str, label_value: str
):
    """
    Label a BigQuery table with a specified key-value pair.

    Args:
        project_id (str): The ID of the project containing the BigQuery table.
        dataset_id (str): The ID of the dataset containing the BigQuery table.
        table_id (str): The ID of the BigQuery table to label.
        label_key (str): The key of the label to add.
        label_value (str): The value of the label to add.

    Returns:
        CompletedProcess: Result of the subprocess run.

    Raises:
        CalledProcessError: If an error occurs during the subprocess run.
    """

    command = [
        "bq",
        "update",
        "--set_label",
        f"{label_key}:{label_value}",
        f"{project_id}:{dataset_id}.{table_id}",
    ]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        log(f"{result.stdout}Value: {label_key}: {label_value}", level="info")
    except subprocess.CalledProcessError as e:
        log(f"Error adding label: {e.stderr}", level="error")
