# -*- coding: utf-8 -*-
import datetime
import io
import chardet

import pandas as pd
import pytz
from tenacity import retry, stop_after_attempt, wait_fixed
from unidecode import unidecode

from pipelines.utils.logger import log


def fix_csv(csv_text: str, sep: str) -> str:
    csv_text = csv_text.replace("\r\n", "\n")

    first_line = csv_text.splitlines()[0]

    columns = first_line.split(sep)

    other_lines = csv_text.splitlines()[1:]

    max_cols = len(columns)
    for line in other_lines:
        line_columns = line.split(",")

        if len(line_columns) > max_cols:
            max_cols = len(line_columns)

    diff = max_cols - len(columns)

    for i in range(diff):
        columns.append(f"complemento_{i}")

    new_first_line = sep.join(columns)
    new_csv_text = new_first_line + "\n" + "\n".join(other_lines)

    return new_csv_text


def fix_column_name(column_name: str) -> str:
    c = (
        column_name.replace("(", "")
        .replace(")", "")
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("/", "_")
        .replace(",", "_")
        .replace("[", "_")
        .replace("]", "_")
        .lower()
    )

    return unidecode(c)


def detect_separator(csv_text: str) -> str:
    first_line = csv_text.splitlines()[0]
    if len(first_line.split(",")) > len(first_line.split(";")):
        return ","
    else:
        return ";"


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def download_file(bucket, file_name):
    blob = bucket.get_blob(file_name)
    size_in_bytes = blob.size
    size_in_mb = size_in_bytes / (1024 * 1024)

    log(f"Beginning Download of {file_name} with {size_in_mb:.1f} MB")
    data = blob.download_as_bytes()
    detected_encoding = chardet.detect(data)['encoding']
    csv_text = data.decode(detected_encoding)

    sep = detect_separator(csv_text)

    # Fix CSV
    csv_text = fix_csv(csv_text, sep)
    csv_file = io.StringIO(csv_text)

    # Read CSV
    try:
        df = pd.read_csv(csv_file, sep=sep, dtype=str, encoding="utf-8")
    except pd.errors.ParserError:
        log("Error reading CSV file")
        return pd.DataFrame()

    df.columns = [fix_column_name(column) for column in df.columns]

    df["_source_file"] = file_name
    df["_extracted_at"] = blob.updated.astimezone(tz=pytz.timezone("America/Sao_Paulo"))
    df["_loaded_at"] = datetime.datetime.now(tz=pytz.timezone("America/Sao_Paulo"))

    log(f"Finishing Download of {file_name}")
    return df
