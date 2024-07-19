# -*- coding: utf-8 -*-
"""
Data transformation functions to vitalcare API data
"""

import ast

import pandas as pd
from prefeitura_rio.pipelines_utils.logging import log


def fix_payload_column_order(filepath: str, table_id: str, sep: str = ";"):
    """
    Load a CSV file into a pandas DataFrame, keeping all column types as string,
    and reorder the columns in a specified order.

    Parameters:
    - filepath: str
        The file path of the CSV file to load.

    Returns:
    - DataFrame
        The loaded DataFrame with columns reordered.
    """
    columns_order = {
        "estoque_posicao": [
            "ap",
            "cnesUnidade",
            "nomeUnidade",
            "desigMedicamento",
            "atc",
            "code",
            "lote",
            "dtaCriLote",
            "dtaValidadeLote",
            "estoqueLote",
            "id",
            "_data_carga",
        ],
        "estoque_movimento": [
            "ap",
            "cnesUnidade",
            "nomeUnidade",
            "desigMedicamento",
            "atc",
            "code",
            "lote",
            "dtaMovimento",
            "tipoMovimento",
            "motivoCorrecao",
            "justificativa",
            "cpfProfPrescritor",
            "cnsProfPrescritor",
            "cpfPatient",
            "cnsPatient",
            "qtd",
            "codWms",
            "armazemOrigem",
            "armazemDestino",
            "id",
            "_data_carga",
        ],
    }

    # Specifying dtype as str to ensure all columns are read as strings
    df = pd.read_csv(filepath, sep=sep, dtype=str, encoding="utf-8")

    # Specifying the desired column order
    column_order = columns_order[table_id]

    # Reordering the columns
    df = df[column_order]

    df.to_csv(filepath, sep=sep, index=False, encoding="utf-8")

    log(f"Columns reordered for {filepath}")


def from_json_to_csv(input_path, sep=";"):
    """
    Converts a JSON file to a CSV file.

    Args:
        input_path (str): The path to the input JSON file.
        sep (str, optional): The separator to use in the output CSV file. Defaults to ";".

    Returns:
        str: The path to the output CSV file, or None if an error occurred.
    """
    try:
        output_path = input_path.replace(".json", ".csv")
        with open(input_path, "r", encoding="utf-8") as file:
            content = file.read()

            # Remove trailing comma from the string
            content = content.strip(",")

            # Parse string to list of dictionaries
            data = ast.literal_eval(content)

            # Assuming the JSON structure is a list of dictionaries
            df = pd.DataFrame(data, dtype="str")
            df.to_csv(output_path, index=False, sep=sep, encoding="utf-8")

            log("JSON converted to CSV")
            return output_path

    except Exception as e:  # pylint: disable=W0703
        log(f"An error occurred: {e}", level="error")
        return None
