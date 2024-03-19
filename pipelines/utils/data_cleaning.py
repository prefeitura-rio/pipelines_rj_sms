# -*- coding: utf-8 -*-
"""
Utilities for data cleaning.
"""

import re

import pandas as pd


def final_column_treatment(column: str) -> str:
    """
    Adds an underline before column name if it only has numbers or remove all non alpha numeric
    characters besides underlines ("_").
    """
    try:
        int(column)
        return f"_{column}"
    except ValueError:  # pylint: disable=bare-except
        non_alpha_removed = re.sub(r"[\W]+", "", column)
        return non_alpha_removed


def remove_columns_accents(dataframe: pd.DataFrame) -> list:
    """
    Remove accents from dataframe columns.
    """
    columns = [str(column) for column in dataframe.columns]
    dataframe.columns = columns
    return list(
        dataframe.columns.str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .map(lambda x: x.strip())
        .str.replace(" ", "_")
        .str.replace("/", "_")
        .str.replace("-", "_")
        .str.replace("\a", "_")
        .str.replace("\b", "_")
        .str.replace("\n", "_")
        .str.replace("\t", "_")
        .str.replace("\v", "_")
        .str.replace("\f", "_")
        .str.replace("\r", "_")
        .str.lower()
        .map(final_column_treatment)
    )
