# -*- coding: utf-8 -*-
# pylint: disable= C0301
"""
Tasks for dump_api_vitacare
"""

from datetime import (
    date,
    datetime,
    timedelta,
)


import pandas as pd
import prefect
from prefect import task
from prefect.client import Client
from prefeitura_rio.pipelines_utils.logging import log
from google.cloud import bigquery

from pipelines.dump_api_vitacare.constants import (
    constants as vitacare_constants,
)
from pipelines.utils.tasks import from_json_to_csv, add_load_date_column, save_to_file


@task
def rename_current_flow(table_id: str, ap: str, cnes: str = None):
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()
    if cnes:
        return client.set_flow_run_name(flow_run_id, f"{table_id}.ap{ap}")
    else:
        return client.set_flow_run_name(flow_run_id, f"{table_id}.ap{ap}.cnes_{cnes}")


@task
def build_url(ap: str, endpoint: str) -> str:
    """
    Build the URL for the given AP and endpoint.

    Args:
        ap (str): The AP name.
        endpoint (str): The endpoint name.

    Returns:
        str: The built URL.

    """
    url = f"{vitacare_constants.BASE_URL.value[ap]}{vitacare_constants.ENDPOINT.value[endpoint]}"  # noqa: E501
    log(f"URL built: {url}")
    return url


@task
def build_params(date_param: str = "today", cnes: str = None) -> dict:
    """
    Build the parameters for the API request.

    Args:
        date_param (str, optional): The date parameter. Defaults to "today".
        cnes (str, optional): The CNES ID. Defaults to None.

    Returns:
        dict: The parameters for the API request.
    """
    if date_param == "today":
        params = {"date": str(date.today())}
    elif date_param == "yesterday":
        params = {"date": str(date.today() - timedelta(days=1))}
    else:
        try:
            # check if date_param is a date string
            datetime.strptime(date_param, "%Y-%m-%d")
            params = {"date": date_param}
        except ValueError as e:
            raise ValueError("date_param must be a date string (YYYY-MM-DD)") from e

    if cnes:
        params.update({"cnes": cnes})

    log(f"Params built: {params}")
    return params


@task
def create_filename(table_id: str, ap: str) -> str:
    return f"{table_id}_ap{ap}"


@task
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
            "cnsProfPrescritor",
            "cpfPatient",
            "cnsPatient",
            "qtd",
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


@task
def save_data_to_file(
    data: str,
    file_folder: str,
    table_id: str,
    ap: str,
    cnes: str = None,
    add_load_date_to_filename: bool = False,
    load_date: str = None,
):
    """
    Save data to a file.

    Args:
        data (str): The data to be saved.
        file_folder (str): The folder where the file will be saved.
        table_id (str): The table ID.
        ap (str): The AP value.
        cnes (str, optional): The CNES value. Defaults to None.
        add_load_date_to_filename (bool, optional): Whether to add the load date to the filename. Defaults to False.  # noqa: E501
        load_date (str, optional): The load date. Defaults to None.

    Returns:
        bool: True if the data was successfully saved, False otherwise.
    """
    if cnes:
        file_name = f"{table_id}__ap{ap}__cnes{cnes}"
    else:
        file_name = f"{table_id}_ap{ap}"

    file_path = save_to_file.run(
        data=data,
        file_folder=file_folder,
        file_name=file_name,
        add_load_date_to_filename=add_load_date_to_filename,
        load_date=load_date,
    )

    with open(file_path, "r", encoding="UTF-8") as f:
        first_line = f.readline().strip()

    if first_line == "[]":
        log("The json content is empty.")
        return False
    else:
        csv_file_path = from_json_to_csv.run(input_path=file_path, sep=";")

        add_load_date_column.run(input_path=csv_file_path, sep=";")

        fix_payload_column_order.run(filepath=csv_file_path, table_id=table_id)

        return True


@task
def retrieve_cases_to_reprocessed_from_birgquery():
    # Define your BigQuery client
    client = bigquery.Client()

    # Specify your dataset and table
    dataset_id = "controle_reprocessamento"
    table_id = "brutos_prontuario_vitacare__estoque_movimento"
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    retrieve_query = f"SELECT * FROM `{full_table_id}` LIMIT 2"

    query_job = client.query(retrieve_query)
    query_job.result()

    data_list = []
    for i, row in enumerate(query_job):
        # Here, we're using a simple integer index as the key
        # You can replace this with a unique identifier from your row, if available
        data_list.append(dict(row))

    log(f"{len(data_list)} rows retrieved from BigQuery.")

    return data_list


@task
def build_params_reprocess(
    environment: str, ap: str, endpoint: str, table_id: str, data: str, cnes: str
) -> dict:
    """
    Build the parameters for the API request.

    Args:
        date_param (str, optional): The date parameter. Defaults to "today".
        cnes (str, optional): The CNES ID. Defaults to None.

    Returns:
        dict: The parameters for the API request.
    """
    params = {
        "environment": environment,
        "ap": ap,
        "endpoint": endpoint,
        "table_id": table_id,
        "date": data,
        "cnes": cnes,
        "rename_flow": True,
    }

    log(f"Params built: {params}")
    return params
