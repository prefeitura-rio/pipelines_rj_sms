# -*- coding: utf-8 -*-
"""
SISREG dumping tasks
"""
import os
from datetime import timedelta

from prefect.engine.signals import FAIL
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.sisreg_web_v2.constants import (
    constants as sisreg_constants,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_componentes.core.sisreg_app import (
    SisregApp,
)
from pipelines.datalake.extract_load.sisreg_web_v2.sisreg.sisreg_componentes.utils.path_utils import (
    definir_caminho_absoluto,
)
from pipelines.datalake.utils.data_transformations import (
    conform_header_to_datalake,
    convert_to_parquet,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import add_load_date_column, get_secret_key


@task(max_retries=5, retry_delay=timedelta(minutes=3))
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

    caminho_download = definir_caminho_absoluto("downloaded_data/")
    sisreg = SisregApp(
        usuario=username,
        senha=password,
        caminho_download=caminho_download,
    )

    sisreg.fazer_login()

    # Todo: Abstrair / encapsular a logica a seguir (transformar em função)
    string_debug = "oferta_programada"
    log(f"Endpoint: {endpoint} == String: {string_debug}: {endpoint == string_debug}", level="debug") #Apenas depurando o codigo
    if endpoint == string_debug :
        log(f"Starting download - {endpoint} - To: {caminho_download}", level="debug")
        sisreg.extrair_oferta_programada(caminho_download=caminho_download)
        log(f"File downloaded to: {caminho_download}", level="debug")

        if caminho_download:
            sisreg.encerrar()  # Todo: rever logica de encerramento do browser (repetindo d+)

            return caminho_download
        else:
            sisreg.encerrar()
            log("Error downloading file", level="error")
            raise FAIL("Error downloading file")

    else:
        sisreg.encerrar()
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

    if endpoint == "oferta_programada":
        # remove timestamp from file name
        os.rename(parquet_file_path, parquet_file_path[:-17] + ".parquet")
        parquet_file_path = parquet_file_path[:-17] + ".parquet"

        log(f"File renamed to: {os.path.basename(parquet_file_path)}")

    return parquet_file_path
