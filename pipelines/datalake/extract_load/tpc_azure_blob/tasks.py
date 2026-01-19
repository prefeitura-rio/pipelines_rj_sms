# -*- coding: utf-8 -*-
# pylint: disable= C0301
# flake8: noqa E501
"""
Tasks for TPC Dump
"""

import csv
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import pytz
from prefect.engine.signals import ENDRUN
from prefect.engine.state import Failed
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.tpc_azure_blob.constants import (
    constants as tpc_constants,
)
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import download_azure_blob, get_secret_key


@task(max_retries=4, retry_delay=timedelta(minutes=3))
def extract_data_from_blob(
    blob_file: str, file_folder: str, file_name: str, environment: str = "dev"
) -> str:
    """
    Extracts data from a blob file and saves it to a local file.

    Args:
        blob_file (str): The type of blob file to extract data from.
        file_folder (str): The folder where the downloaded file will be saved.
        file_name (str): The name of the downloaded file.
        environment (str, optional): The environment to use for retrieving the secret key. Defaults to "dev".

    Returns:
        dict: The path of the downloaded file.

    Raises:
        Exception: If the extraction fails after the maximum number of retries.

    """

    container_name = tpc_constants.CONTAINER_NAME.value
    blob_path = tpc_constants.BLOB_PATH.value[blob_file]

    # EXTRACT DATA
    token = get_secret_key.run(
        secret_path=tpc_constants.INFISICAL_PATH.value,
        secret_name=tpc_constants.INFISICAL_TPC_TOKEN.value,
        environment=environment,
    )

    file_path = download_azure_blob.run(
        container_name=container_name,
        blob_path=blob_path,
        file_folder=file_folder,
        file_name=file_name,
        credentials=token,
        add_load_date_to_filename=True,
    )

    # CHECK IF FILE IS FROM CURRENT DATE
    log("Verifying if file is from current date")
    df = pd.read_csv(
        file_path,
        sep=";",
        quoting=csv.QUOTE_MINIMAL,
        encoding="utf-8",
        quotechar='"',
        escapechar="\\",
        keep_default_na=False,
        dtype=str,
        nrows=1,
    )

    replication_date = df.data_atualizacao[0]
    replication_date = pd.Timestamp(replication_date).strftime("%Y-%m-%d")

    log(f"File replication date: {replication_date}")

    if replication_date != pd.Timestamp.now().strftime("%Y-%m-%d"):
        raise ENDRUN(
            state=Failed(f"File is not from current date. Replication date: {replication_date}")
        )
    else:
        log("File is from current date")
        return file_path


@task
def transform_data(file_path: str, blob_file: str):
    """
    Reads a CSV file from the given filepath, applies some data cleaning and formatting operations,
    and saves the resulting dataframe back to the same file in a different format.

    Args:
        filepath (str): The path to the CSV file to be processed.

    Returns:
        None
    """

    log("Conforming CSV to GCP")

    df = pd.read_csv(
        file_path,
        sep=";",
        quoting=csv.QUOTE_MINIMAL,
        encoding="utf-8",
        quotechar='"',
        escapechar="\\",
        keep_default_na=False,
        dtype=str,
    )

    if blob_file == "posicao":
        # remove registros errados
        df = df[df.sku != ""]

        # converte para valores numéricos
        df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
        df["peso_bruto"] = df.peso_bruto.apply(lambda x: float(x.replace(",", ".")))
        df["qtd_dispo"] = df.qtd_dispo.apply(lambda x: float(x.replace(",", ".")))
        df["qtd_roma"] = df.qtd_roma.apply(lambda x: float(x.replace(",", ".")))
        df["preco_unitario"] = df.preco_unitario.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )

        # converte as validades
        df["validade"] = df.validade.apply(lambda x: x[:10])
        df["dt_situacao"] = df.dt_situacao.apply(lambda x: x[-4:] + "-" + x[3:5] + "-" + x[:2])
    elif blob_file == "pedidos":
        # converte para valores numéricos
        df["valor"] = df.valor.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
        df["peso"] = df.peso.apply(lambda x: float(x.replace(",", ".")))
        df["volume"] = df.volume.apply(lambda x: float(x.replace(",", ".")))
        df["quantidade_peca"] = df.quantidade_peca.apply(lambda x: float(x.replace(",", ".")))
        df["valor_total"] = df.valor_total.apply(
            lambda x: float(x.replace(",", ".")) if x != "" else x
        )
    elif blob_file == "recebimento":
        df["qt"] = df.qt.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
        df["qt_fis"] = df.qt_fis.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
        df["pr_unit"] = df.pr_unit.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
        df["vl_merc"] = df.vl_merc.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
        df["vl_total"] = df.vl_total.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)
        df["qt_rec"] = df.qt_rec.apply(lambda x: float(x.replace(",", ".")) if x != "" else x)

    # add load date column
    tz = pytz.timezone("Brazil/East")
    df["_data_carga"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    df.to_csv(file_path, index=False, sep=";", encoding="utf-8", quoting=0, decimal=".")

    log("CSV now conform")
