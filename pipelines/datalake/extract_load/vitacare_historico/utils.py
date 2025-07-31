# -*- coding: utf-8 -*-

from datetime import datetime

import pandas as pd
import pytz

from pipelines.utils.data_cleaning import remove_columns_accents

# --- Funções auxiliares para pré-processamento ---


def clean_ut_id(val):
    """
    Decodifica e limpa valores VARBINARY de 'ut_id'
    """
    if isinstance(val, bytes):
        try:
            return val.decode("utf-16-le", errors="ignore").replace("\x00", "").strip()
        except UnicodeDecodeError:
            return val.decode("latin-1", errors="ignore").replace("\x00", "").strip()
    return str(val).replace("\x00", "").strip()


def transform_dataframe(df: pd.DataFrame, cnes_code: str, db_table: str) -> pd.DataFrame:
    """
    Aplica transformações DataFrames extraídos daq Vitacare
    """
    if df.empty:
        return df

    now = datetime.now(pytz.timezone("America/Sao_Paulo")).replace(tzinfo=None)
    df["extracted_at"] = now
    df["id_cnes"] = cnes_code

    df.columns = remove_columns_accents(df)

    # Aplica clean_ut_id se a coluna existe
    if "ut_id" in df.columns:
        df["ut_id"] = df["ut_id"].apply(clean_ut_id)

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.replace(r"[\n\r\t\x00]+", " ", regex=True)

    return df
