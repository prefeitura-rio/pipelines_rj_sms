# -*- coding: utf-8 -*-
import pandas as pd
from google.cloud import bigquery


def generate_bigquery_schema(df: pd.DataFrame) -> list[bigquery.SchemaField]:
    """
    Generates a BigQuery schema based on the provided DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame for which the BigQuery schema needs to be generated.

    Returns:
        list[bigquery.SchemaField]: The generated BigQuery schema as a list of SchemaField objects.
    """
    TYPE_MAPPING = {
        "i": "INTEGER",
        "u": "NUMERIC",
        "b": "BOOLEAN",
        "f": "FLOAT",
        "O": "STRING",
        "S": "STRING",
        "U": "STRING",
        "M": "TIMESTAMP",
    }
    schema = []
    for column, dtype in df.dtypes.items():
        val = df[column].iloc[0]
        mode = "REPEATED" if isinstance(val, list) else "NULLABLE"

        if isinstance(val, dict) or (mode == "REPEATED" and isinstance(val[0], dict)):
            fields = generate_bigquery_schema(pd.json_normalize(val))
        else:
            fields = ()

        _type = "RECORD" if fields else TYPE_MAPPING.get(dtype.kind)
        schema.append(
            bigquery.SchemaField(
                name=column,
                field_type=_type,
                mode=mode,
                fields=fields,
            )
        )
    return schema
