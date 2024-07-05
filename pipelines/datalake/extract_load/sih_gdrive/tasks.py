# -*- coding: utf-8 -*-
"""
SIH dumping tasks
"""
import os
from typing import List

from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
    convert_to_parquet,
)
from pipelines.utils.credential_injector import authenticated_task as task


@task
def generate_filters(last_update_start_date: str = None, last_update_end_date: str = None):
    """
    Generate filters based on the provided start and end dates.

    Args:
        last_update_start_date (str, optional): The start date for filtering. Defaults to None.
        last_update_end_date (str, optional): The end date for filtering. Defaults to None.

    Returns:
        tuple: A tuple containing the start and end dates if both are provided, otherwise None.
    """
    if last_update_start_date and last_update_end_date:
        return (last_update_start_date, last_update_end_date)

    return None


@task
def transform_data(files_path: List[str]) -> List[str]:
    """
    Transforms the data files in the given list of file paths.

    Args:
        files_path (List[str]): A list of file paths to be transformed.

    Returns:
        List[str]: A list of transformed file paths.

    """
    transformed_files = []

    for file in files_path:

        log(f"Transforming file: {os.path.basename(file)}")

        parquet_file_path = convert_to_parquet(file_path=file, file_type="dbc", encoding="latin1")

        parquet_file_path = conform_header_to_datalake(
            file_path=parquet_file_path,
            file_type="parquet",
        )

        # standardize the file name
        file_path, file_name = os.path.split(parquet_file_path)

        new_file_name = f"{file_name[:4]}_20{file_name[4:6]}-{file_name[6:8]}-01.parquet"
        new_parquet_file_path = os.path.join(file_path, new_file_name)

        os.rename(parquet_file_path, new_parquet_file_path)

        log(f"File transformed to: {os.path.basename(new_file_name)}")

        transformed_files.append(new_parquet_file_path)

    return transformed_files
