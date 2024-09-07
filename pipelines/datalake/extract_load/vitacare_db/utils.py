# -*- coding: utf-8 -*-
# pylint: disable=C0301
# flake8: noqa: E501
"""
Functions for Vitacare db pipeline
"""
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from prefeitura_rio.pipelines_utils.logging import log


def add_flow_metadata(
    file_path: str,
):
    """
    Add flow metadata to a file.

    Parameters:
    file_path (str): The path to the file.
    file_type (str, optional): The type of the file. Defaults to "csv".
    sep (str, optional): The separator used in the file. Defaults to ";".
    snapshot_date (str, optional): The snapshot date to be added as metadata. Defaults to None.
    parse_date_from (str, optional): The source from which to parse the snapshot date. Defaults to None.

    Raises:
    ValueError: If neither snapshot_date nor parse_date_from is provided.
    ValueError: If an invalid value is provided for file_type.
    ValueError: If an invalid value is provided for parse_date_from.
    ValueError: If the date cannot be parsed from the filename.

    Returns:
    str: The path to the file.

    """
    file_path = Path(file_path)

    tz = pytz.timezone("Brazil/East")

    log(f"Adding date metadata to {file_path} ...", level="debug")

    df = pd.read_parquet(file_path)

    df["id_cnes"] = str(file_path.parent).split("/")[-1]
    df["imported_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    df.to_parquet(file_path, index=False)

    return file_path


def fix_parquet_type(file_path):
    """
    Fix the parquet type of a file.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    str: The path to the file.

    """
    file_path = Path(file_path)

    log(f"Fixing parquet type of {file_path} ...", level="debug")

    df = pd.read_parquet(file_path)

    for col in df.columns:
        df[col] = df[col].astype(str)

    df.to_parquet(file_path, index=False)

    return file_path


def rename_file(file_path: str):
    """
    Rename files to a standard format.

    Parameters:
    file_path (str): The path to the file.

    Returns:
    str: The path to the file.

    """
    file_path = Path(file_path)

    log(f"Renaming {file_path} ...", level="debug")

    file_name = file_path.stem
    file_extension = file_path.suffix

    # replace staging_brutos_vitacare_historic_vitacare_historic_ for vitacare_historico
    new_file_name = f"{str(file_path.parent).split('/')[-1]}_{file_name}"

    new_file_path = file_path.parent / f"{new_file_name}{file_extension}"

    file_path.rename(new_file_path)

    return new_file_path
