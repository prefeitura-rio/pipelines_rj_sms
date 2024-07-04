# -*- coding: utf-8 -*-
"""
SIH dumping tasks
"""
import os
from datetime import timedelta

from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg_web.sisreg.sisreg import Sisreg
from pipelines.datalake.utils.data_extraction.google_drive import (
    download_files,
    filter_files_by_date,
    get_files_from_folder,
)
from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
    convert_to_parquet,
)
from pipelines.prontuarios.utils.tasks import get_datetime_working_range
from pipelines.tools.vitacare_healthcheck.tasks import (
    fix_column_typing,
    get_file_content,
    get_structured_files_metadata,
    json_records_to_dataframe,
    loading_data_to_bigquery,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import add_load_date_column


@task
def dowload_from_gdrive(folder_id: str, destination_folder: str) -> str:
    """
    Downloads the file from Google Drive.

    Args:
        file_id (str): The file ID in Google Drive.
        file_path (str): The path to save the downloaded file.

    Returns:
        str: The path of the downloaded file.
    """

    files = get_files_from_folder.run(folder_id=folder_id, file_extension="dbc")

    last_24h_files = filter_files_by_date.run(
        files=files,
        start_datetime="2024-07-04T00:00:00.000Z",  # TODO: remove variable
        end_datetime="2024-07-04T22:19:34.153Z",  # TODO: remove variable
    )

    downloaded_files = download_files.run(files=last_24h_files, folder_path=destination_folder)

    return downloaded_files


@task
def transform_data(file_path: str, env: str) -> list[str]:
    """
    Transforms the data in the given file path and returns the path of the transformed file.

    Args:
        file_path (str): The path of the input file.
        endpoint (str): The endpoint to determine the transformation logic.

    Returns:
        str: The path of the transformed file.
    """

    # list all files in the directory
    raw_files = [file for file in os.listdir(file_path) if file.endswith(".csv")]

    transformed_files = []

    for file in raw_files:

        file_path = add_load_date_column.run(
            input_path=f"{file_path}/{file}", sep=",", encoding="latin1"
        )

        file_path = conform_header_to_datalake(file_path=file_path, file_type="csv", csv_sep=",")

        parquet_file_path = convert_to_parquet(file_path=file_path, csv_sep=",", encoding="utf-8")

        # remove timestamp from file name
        file_path, file_name = os.path.split(parquet_file_path)

        new_file_name = f"{file_name[:4]}_20{file_name[4:6]}-{file_name[6:8]}-01.parquet"
        new_parquet_file_path = os.path.join(file_path, new_file_name)

        os.rename(parquet_file_path, new_parquet_file_path)

        log(f"File renamed to: {os.path.basename(new_file_name)}")

        transformed_files.append(new_parquet_file_path)

    return transformed_files
