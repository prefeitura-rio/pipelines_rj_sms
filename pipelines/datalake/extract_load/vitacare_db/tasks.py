# -*- coding: utf-8 -*-
# pylint: disable=C0103, R0913, C0301, W3101
# flake8: noqa: E501
"""
Tasks for DataSUS pipelines
"""
import os
import shutil
from pathlib import Path
from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log
from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import (
    create_partitions,
    upload_to_datalake,
)
from pipelines.datalake.extract_load.vitacare_db.utils import add_flow_metadata, fix_parquet_type

@task
def load_local_data_to_raw(input_path: str, output_path: str):
    """
    Load local data to raw folder
    """
    output_base_path = Path(output_path)

    for file in Path(input_path).glob("*.parquet"):

        cnes = file.name.split("_")[2]
        endpoint = file.name.split("_")[3].split(".")[0]

        output_path = os.path.join(output_base_path, endpoint)

        if not os.path.exists(output_path):
            os.makedirs(output_path)
        shutil.copy(file, Path(output_path))

    raw_files = list(Path(output_base_path).rglob("*.parquet"))

    log("Files copied successfully")

    return raw_files


@task
def transform_data(files_path: list[str], env: str):
    """
    Transforms data from the DATASUS FTP server.
    """

    raw_files = files_path

    transformed_files = [fix_parquet_type(file_path=file) for file in raw_files]

    transformed_files = [add_flow_metadata(file_path=file) for file in transformed_files]

    transformed_files = [
        conform_header_to_datalake(file_path=file, file_type="parquet")
        for file in transformed_files
    ]

    log("Header conformed successfully")

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
    source_format="parquet",
    if_exists="replace",
    if_storage_data_exists="replace",
    biglake_table=True,
    dataset_is_public=False,
):
    """
    Uploads different tables to data lake.
    """

    tables_path = [x[0] for x in os.walk(input_path)][1:]

    for table in tables_path:
        table_name = f'{table.split("/")[-1]}_historico'
        upload_to_datalake.run(
            input_path=table,
            dataset_id=dataset_id,
            table_id=table_name,
            dump_mode="overwrite",
            source_format=source_format,
            if_exists=if_exists,
            if_storage_data_exists=if_storage_data_exists,
            biglake_table=biglake_table,
            dataset_is_public=dataset_is_public,
        )
    log("Files uploaded successfully", level="info")
