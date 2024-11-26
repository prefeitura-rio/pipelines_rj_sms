# -*- coding: utf-8 -*-
# pylint: disable= C0301
# flake8: noqa: E501
"""
Data transformations functions for data lake
"""

import os
import re
from datetime import date, datetime, timedelta

import pandas as pd
import pyreaddbc
import pytz
from prefeitura_rio.pipelines_utils.logging import log
from simpledbf import Dbf5

from pipelines.utils.data_cleaning import remove_columns_accents


def convert_str_to_date(target_date: str) -> str:
    """
    Convert a string to a date in the format YYYY-MM-DD

    :param target_date: The target date to convert
    :type target_date: str
    :return: The target date in the format YYYY-MM-DD
    :rtype: str
    """
    if target_date == "today":
        return date.today().strftime("%Y-%m-%d")
    elif target_date == "yesterday":
        return (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    elif target_date.startswith("d-"):
        days = int(target_date.replace("d-", ""))
        return (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    else:
        try:
            return target_date
        except ValueError as e:
            raise ValueError("The target_date must be in the format YYYY-MM-DD") from e


def conform_header_to_datalake(
    file_path: str, file_type: str = "csv", csv_sep: str = ";", encoding: str = "utf-8"
) -> None:
    """
    Conforms the header of a file to the datalake format.

    Args:
        file_path (str): The path to the file.
        file_type (str, optional): The type of the file. Defaults to "csv".
        csv_sep (str, optional): The separator used in CSV files. Defaults to ";".
        encoding (str, optional): The encoding of the file. Defaults to "utf-8".

    Raises:
        ValueError: If the file type is not 'csv'.

    Returns:
        str: The file path.
    """

    if file_type == "csv":
        dataframe = pd.read_csv(
            file_path,
            sep=csv_sep,
            dtype=str,
            keep_default_na=False,
            encoding=encoding,
        )
    elif file_type == "parquet":
        dataframe = pd.read_parquet(file_path)
    else:
        log(f"File type {file_type} not found", level="error")
        raise ValueError("The file type must be 'csv'")

    # conform header to datalake format
    dataframe.columns = remove_columns_accents(dataframe)

    # save file
    if file_type == "csv":
        dataframe.to_csv(file_path, index=False, sep=csv_sep, encoding=encoding)

    elif file_type == "parquet":
        dataframe.to_parquet(file_path, index=False)

    return file_path


def convert_to_parquet(
    file_path: str, file_type: str = "csv", csv_sep: str = ";", encoding: str = "utf-8"
) -> str:
    """
    Converts a file to Parquet format.

    Args:
        file_path (str): The path to the input file.
        file_type (str, optional): The type of the input file. Defaults to "csv".
        csv_sep (str, optional): The separator used in the CSV file. Defaults to ";".
        encoding (str, optional): The encoding of the input file. Defaults to "utf-8".

    Returns:
        str: The path to the converted Parquet file.
    """

    match file_type:
        case "csv":
            dataframe = pd.read_csv(
                file_path,
                sep=csv_sep,
                dtype=str,
                keep_default_na=False,
                encoding=encoding,
            )
        case "dbc":
            dbf_file = file_path.replace(".dbc", ".dbf")
            pyreaddbc.dbc2dbf(file_path, dbf_file)
            dbf = Dbf5(dbf_file, codec=encoding)
            dataframe = dbf.to_dataframe(na="")
            dataframe = dataframe.astype(str)
        case "dbf":
            dbf = Dbf5(file_path, codec=encoding)
            dataframe = dbf.to_dataframe(na="")
            dataframe = dataframe.astype(str)
        case "xlsx":
            dataframe = pd.read_excel(file_path, keep_default_na=False, dtype=str)
        case _:
            log(f"File type {file_type} not found", level="error")
            raise ValueError("The file type must be 'csv', 'dbc', 'dbf' or 'xlsx'")

    parquet_file_path = file_path.replace(f".{file_type}", ".parquet")
    dataframe.to_parquet(parquet_file_path, index=False)

    return parquet_file_path


def add_flow_metadata(
    file_path: str,
    file_type: str = "csv",
    sep=";",
    snapshot_date: str = None,
    parse_date_from: str = None,
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

    if snapshot_date is None and parse_date_from is None:
        raise ValueError("Either snapshot_date or parse_date_from must be provided")

    tz = pytz.timezone("Brazil/East")

    log(f"Adding date metadata to {file_path} ...", level="debug")

    if file_type == "csv":
        df = pd.read_csv(file_path, sep=sep, keep_default_na=False, dtype="str")
    elif file_type == "parquet":
        df = pd.read_parquet(file_path)
    else:
        log(f"Invalid value for file_type: {file_type}", level="error")
        raise ValueError("Invalid value for file_type")

    df["imported_at"] = datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")

    if snapshot_date is not None:
        df["created_at"] = snapshot_date
    else:
        if parse_date_from == "filename":
            snapshot_date = re.findall(r"\d{6}", file_path)[0]
            snapshot_date = f"{snapshot_date[:4]}-{snapshot_date[-2:]}"
        elif parse_date_from == "last_modified":
            snapshot_date = datetime.fromtimestamp(os.path.getmtime(file_path)).strftime("%Y-%m-%d")
        else:
            raise ValueError("Invalid value for parse_date_from")

        if date:
            log(f"Date parsed from filename: {snapshot_date}", level="debug")
            df["_data_snapshot"] = snapshot_date
        else:
            log("Failed to parse date from filename", level="error")
            raise ValueError("Failed to parse date from filename")

    if file_type == "csv":
        df.to_csv(file_path, index=False, sep=sep, encoding="utf-8")
    elif file_type == "parquet":
        df.to_parquet(file_path, index=False)

    return file_path
