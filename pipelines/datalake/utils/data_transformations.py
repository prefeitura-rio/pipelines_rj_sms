# -*- coding: utf-8 -*-
# pylint: disable= C0301
"""
Data transformations functions for data lake
"""

from datetime import date, timedelta

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log

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
    else:
        try:
            return target_date
        except ValueError as e:
            raise ValueError("The target_date must be in the format YYYY-MM-DD") from e


def conform_header_to_datalake(file_path: str, file_type: str = "csv", csv_sep: str = ";") -> None:
    """
    Conforms the header of a file to the datalake format.

    Args:
        file_path (str): The path to the file.
        file_type (str, optional): The type of the file. Defaults to "csv".
        csv_sep (str, optional): The separator used in the CSV file. Defaults to ";".

    Returns:
        The path to the file

    Raises:
        ValueError: If the file type is not 'csv'.
    """
    if file_type == "csv":
        dataframe = pd.read_csv(
            file_path,
            sep=csv_sep,
            dtype=str,
            keep_default_na=False,
            encoding="utf-8",
        )
        dataframe.columns = remove_columns_accents(dataframe)
        dataframe.to_csv(file_path, index=False, sep=csv_sep, encoding="utf-8")
        return file_path
    else:
        log(f"File type {file_type} not found", level="error")
        raise ValueError("The file type must be 'csv'")


def convert_to_parquet(file_path: str, csv_sep: str = ";", encoding: str = "utf-8") -> None:
    """
    Converts a CSV file to Parquet format.

    Args:
        file_path (str): The path to the CSV file.
        csv_sep (str, optional): The separator used in the CSV file. Defaults to ";".
        encoding (str, optional): The encoding of the CSV file. Defaults to "utf-8".

    Returns:
        str: The path to the converted Parquet file.
    """

    dataframe = pd.read_csv(
        file_path,
        sep=csv_sep,
        dtype=str,
        keep_default_na=False,
        encoding=encoding,
    )

    parquet_file_path = file_path.replace(".csv", ".parquet")
    dataframe.to_parquet(parquet_file_path, index=False)

    log(f"File converted to Parquet: {parquet_file_path}")

    return parquet_file_path
