# -*- coding: utf-8 -*-
"""
SISREG dumping tasks
"""

import os
from datetime import timedelta

from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg_web.constants import (
    constants as sisreg_constants,
)
from pipelines.datalake.extract_load.sisreg_web.sisreg.sisreg import Sisreg
from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
    convert_to_parquet,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import add_load_date_column, get_secret_key


@task(max_retries=2, retry_delay=timedelta(minutes=3))
def extract_data_from_sisreg(environment: str, endpoint: str, download_path: str) -> None:
    """
    Extracts data from SISREG website.

    Args:
        environment (str): The environment to use for accessing the SISREG website.
        endpoint (str): The endpoint to extract data from.
        download_path (str): The path to download the extracted data.

    Returns:
        None: It either returns the file path of the downloaded data or raises an exception.

    Raises:
        FAIL: If the specified endpoint is not found.

    """
    username = get_secret_key.run(
        secret_path=sisreg_constants.INFISICAL_PATH.value,
        secret_name=sisreg_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=environment,
    )
    password = get_secret_key.run(
        secret_path=sisreg_constants.INFISICAL_PATH.value,
        secret_name=sisreg_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=environment,
    )

    log("Acessing SISREG website")

    sisreg = Sisreg(
        user=username,
        password=password,
        download_path=download_path,
    )
    sisreg.login()

    if endpoint == "escala":
        file_path = sisreg.download_escala()
        log(f"File downloaded to: {file_path}", level="debug")
        if file_path:
            return file_path
        else:
            log("Error downloading file", level="error")
            raise FAIL("Error downloading file")
    else:
        log(f"Endpoint {endpoint} not found", level="error")
        raise FAIL(f"Endpoint {endpoint} not found")


@task()
def transform_data(file_path: str, endpoint: str) -> str:
    """
    Transforms the data in the given file path and returns the path of the transformed file.

    Args:
        file_path (str): The path of the input file.
        endpoint (str): The endpoint to determine the transformation logic.

    Returns:
        str: The path of the transformed file.
    """

    file_path = add_load_date_column.run(input_path=file_path, sep=";")

    file_path = conform_header_to_datalake(file_path=file_path, file_type="csv", csv_sep=";")

    parquet_file_path = convert_to_parquet(file_path=file_path, csv_sep=";", encoding="utf-8")

    if endpoint == "escala":
        # remove timestamp from file name
        os.rename(parquet_file_path, parquet_file_path[:-17] + ".parquet")
        parquet_file_path = parquet_file_path[:-17] + ".parquet"

        log(f"File renamed to: {os.path.basename(parquet_file_path)}")

    return parquet_file_path
