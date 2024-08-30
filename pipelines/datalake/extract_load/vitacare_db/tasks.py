# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Tasks for Vitacare db pipeline
"""
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.vitacare_db.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_db.utils import (
    add_flow_metadata,
    fix_parquet_type,
    rename_file,
)
from pipelines.datalake.utils.data_transformations import conform_header_to_datalake
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.googleutils import download_from_cloud_storage
from pipelines.utils.tasks import upload_to_datalake


@task
def get_bucket_name(env: str):
    """
    Get the bucket name based on the environment.
    """

    if env in ["dev", "prod"]:
        bucket_name = vitacare_constants.GCS_BUCKET.value[env]
    else:
        raise FAIL("Invalid environment")

    return bucket_name


@task
def download_from_cloud_storage_task(path: str, bucket_name: str, blob_prefix: str = None):
    """
    Downloads files from Google Cloud Storage to the specified local path.
    """

    blob_prefix = f"parquet/{blob_prefix}"

    downloaded_files = download_from_cloud_storage(
        path=path,
        bucket_name=bucket_name,
        blob_prefix=blob_prefix,
    )

    if len(downloaded_files) == 13:
        log("Files downloaded successfully")
        return downloaded_files
    else:
        log(f"Downloaded {len(downloaded_files)} files, expected 13", level="error")
        raise FAIL(f"Downloaded {len(downloaded_files)} files, expected 13")


@task
def transform_data(files_path: list[str]):
    """
    Transforms data from the DATASUS FTP server.
    """

    raw_files = files_path

    transformed_files = [fix_parquet_type(file_path=file) for file in raw_files]

    log("Parquet type fixed successfully")

    transformed_files = [add_flow_metadata(file_path=file) for file in transformed_files]

    log("Metadata added successfully")

    transformed_files = [
        conform_header_to_datalake(file_path=file, file_type="parquet")
        for file in transformed_files
    ]

    log("Header conformed successfully")

    transformed_files = [rename_file(file_path=file) for file in transformed_files]

    return transformed_files


@task
def upload_many_to_datalake(
    input_path: str,
    dataset_id: str,
    source_format="parquet",
    if_exists="replace",
    if_storage_data_exists="replace",
    biglake_table=True,
    dataset_is_public=False,
):
    """
    Uploads different tables to data lake.
    """

    for table in input_path:
        table_name = f'{table.stem.split("_")[-1]}_historico'
        upload_to_datalake.run(
            input_path=table,
            dataset_id=dataset_id,
            table_id=table_name,
            dump_mode="append",
            source_format=source_format,
            if_exists=if_exists,
            if_storage_data_exists=if_storage_data_exists,
            biglake_table=biglake_table,
            dataset_is_public=dataset_is_public,
        )
    log("Files uploaded successfully", level="info")
