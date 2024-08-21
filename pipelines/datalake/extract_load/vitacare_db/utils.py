import os
import re
from datetime import date, datetime, timedelta
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
    file_name = file_path.name
    cnes = file_name.split("_")[2]

    tz = pytz.timezone("Brazil/East")

    log(f"Adding date metadata to {file_path} ...", level="debug")

    df = pd.read_parquet(file_path)

    df["imported_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
    df["cnes"] = cnes
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