# -*- coding: utf-8 -*-
# pylint: disable= C0301
"""
Tasks for dump_api_vitacare
"""

import hashlib
import json
import os
import re
import shutil
from datetime import date, datetime, timedelta
from functools import partial
from pathlib import Path

import pandas as pd
import prefect
from google.cloud import bigquery
from prefect import task
from prefect.client import Client
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.dump_api_vitacare.constants import constants as vitacare_constants
from pipelines.utils.state_handlers import on_fail, on_success
from pipelines.utils.tasks import add_load_date_column, save_to_file


@task
def rename_current_flow(table_id: str, ap: str, date_param: str = "today", cnes: str = None):
    """
    Rename the current flow run.
    """
    flow_run_id = prefect.context.get("flow_run_id")
    client = Client()

    if date_param == "today":
        data = str(date.today())
    elif date_param == "yesterday":
        data = str(date.today() - timedelta(days=1))
    else:
        try:
            # check if date_param is a date string
            datetime.strptime(date_param, "%Y-%m-%d")
            data = date_param
        except ValueError as e:
            raise ValueError("date_param must be a date string (YYYY-MM-DD)") from e

    if cnes:
        return client.set_flow_run_name(flow_run_id, f"{table_id}__ap_{ap}.cnes_{cnes}__{data}")
    else:
        return client.set_flow_run_name(flow_run_id, f"{table_id}__ap_{ap}__{data}")


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
    """create filename for the downloaded file"""
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
        with open(input_path, "r", encoding="utf-8") as file:
            json_data = file.read()
            data = json.loads(json_data)  # Convert JSON string to Python dictionary
            output_path = input_path.replace(".json", ".csv")
            # Assuming the JSON structure is a list of dictionaries
            df = pd.DataFrame(data, dtype="str")
            df.to_csv(output_path, index=False, sep=sep, encoding="utf-8")

            log("JSON converted to CSV")
            return output_path

    except Exception as e:  # pylint: disable=W0703
        log(f"An error occurred: {e}", level="error")
        return None


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

    log(f"Data saved to file: {file_path}")

    with open(file_path, "r", encoding="UTF-8") as f:
        first_line = f.readline().strip()

    if first_line == "[]":
        log("The json content is empty.")
        return False
    else:

        log("Json not empty")

        csv_file_path = from_json_to_csv.run(input_path=file_path, sep=";")

        add_load_date_column.run(input_path=csv_file_path, sep=";")

        fix_payload_column_order.run(filepath=csv_file_path, table_id=table_id)

        return True


@task
def create_partitions(data_path: str, partition_directory: str):
    """
    Create partitions for the given data files and copy them to the specified partition directory.

    Args:
        data_path (str): The path to the data file(s) or directory containing the data files.
        partition_directory (str): The directory where the partitions will be created.

    Raises:
        ValueError: If the filename does not contain a date in the format YYYY-MM-DD.

    Returns:
        None
    """
    # check if data_path is a directory or a file
    if os.path.isdir(data_path):
        data_path = Path(data_path)
        files = data_path.glob("*.csv")
    else:
        files = [data_path]

    # Create partition directories for each file
    for file_name in files:

        try:
            date_str = re.search(r"\d{4}-\d{2}-\d{2}", str(file_name)).group()
            parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            log(
                "Filename must contain a date in the format YYYY-MM-DD to match partition level",  # noqa: E501
                level="error",
            )  # noqa: E501")

        ano_particao = parsed_date.strftime("%Y")
        mes_particao = parsed_date.strftime("%m")
        data_particao = parsed_date.strftime("%Y-%m-%d")

        output_directory = f"{partition_directory}/ano_particao={int(ano_particao)}/mes_particao={int(mes_particao)}/data_particao={data_particao}"  # noqa: E501

        # Create partition directory
        if not os.path.exists(output_directory):
            os.makedirs(output_directory, exist_ok=False)

        # Copy file(s) to partition directory
        shutil.copy(file_name, output_directory)


@task
def retrieve_cases_to_reprocessed_from_birgquery(dataset_id: str, table_id: str) -> list:
    """
    Retrieves cases to be reprocessed from BigQuery.

    Returns:
        list: A list of dictionaries representing the retrieved rows from BigQuery.
    """
    # Define your BigQuery client
    client = bigquery.Client()

    # Specify your dataset and table
    dataset_id = "controle_reprocessamento"
    table_id = f"{dataset_id}__{table_id}"
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    retrieve_query = f"SELECT * FROM `{full_table_id}` WHERE reprocessing_status = 'pending'"

    query_job = client.query(retrieve_query)
    query_job.result()

    data_list = []
    for row in query_job:
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
        "rename_flow": False,
    }

    log(f"Params built: {params}")
    return params


@task
def creat_multiples_flows_runs(
    run_list: list, environment: str, table_id: str, endpoint: str, parallel_runs: int = 10
):
    """
    Create multiple flow runs based on the given run list.

    Args:
        run_list (list): A list of runs containing information such as area_programatica, data, id_cnes, etc.
        environment (str): The environment to run the flows in.
        table_id (str): The ID of the table to process.
        endpoint (str): The endpoint to use for processing.

    Returns:
        None
    """  # noqa: E501

    start_time = datetime.now() + timedelta(minutes=1)
    parallel_runs_counter = 0
    count = 0

    for run in run_list:
        params = build_params_reprocess.run(
            environment=environment,
            ap=run["area_programatica"],
            endpoint=endpoint,
            table_id=table_id,
            data=run["data"].strftime("%Y-%m-%d"),
            cnes=run["id_cnes"],
        )

        idempotency_key = hashlib.sha256(json.dumps(params, sort_keys=True).encode()).hexdigest()

        create_flow_run.run(
            flow_name="Dump Vitacare - Ingerir dados do prontuário Vitacare",
            project_name="staging",
            parameters=params,
            run_name=f"REPROCESS: {table_id}__ap_{run['area_programatica']}.cnes_{run['id_cnes']}__{run['data'].strftime('%Y-%m-%d')}",  # noqa: E501
            idempotency_key=idempotency_key,
            scheduled_start_time=start_time + timedelta(minutes=2 * count),
        )

        parallel_runs_counter += 1

        if parallel_runs_counter == parallel_runs:
            parallel_runs_counter = 0
            count += 1


@task
def log_error():
    log("State Handler: ERROR")


@task
def log_success():
    log("State Handler: SUCCESS")


@task
def write_on_bq_on_table(
    records_to_update: list, dataset_id: str, table_id: str, status_code: str, row_count: int
):
    # Define your BigQuery client
    client = bigquery.Client()

    # Specify your dataset and table
    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    if status_code == "200":
        reprocessing_status = "success"
    else:
        reprocessing_status = "failed"
        row_count = 0

    for n, _ in enumerate(records_to_update):
        records_to_update[n].update(
            {
                "reprocessing_status": reprocessing_status,
                "request_response_code": status_code,
                "request_row_count": row_count,
            }
        )

        # Write to BigQuery
        # Data to be upserted
        merge_query = f"""
            MERGE `{full_table_id}` T
            USING (SELECT * FROM UNNEST([{', '.join(['STRUCT<id_cnes STRING, data DATE, reprocessing_status STRING, request_response_code STRING, request_row_count INT64>' + str((row['id_cnes'], row['data'], row['reprocessing_status'], row['request_response_code'], row['request_row_count'])) for row in records_to_update])}])) S
            ON T.id_cnes = S.id_cnes AND T.data = S.data
            WHEN MATCHED THEN
            UPDATE SET
                T.reprocessing_status = S.reprocessing_status,
                T.request_response_code = S.request_response_code,
                T.request_row_count = S.request_row_count
            WHEN NOT MATCHED THEN
            INSERT (id_cnes, data, reprocessing_status, request_response_code, request_row_count)
            VALUES(id_cnes, data, reprocessing_status, request_response_code, request_row_count)
            """  # noqa: E501

        # Run the query
        query_job = client.query(merge_query)
        query_job.result()  # Wait for the job to complete

        print("Upsert operation completed.")


@task(
    state_handlers=[
        partial(on_fail, task_to_run_on_fail=log_error),
        partial(on_success, task_to_run_on_success=log_success),
    ]
)
def wait_flor_flow_task(flow_to_wait):
    wait_for_flow_run.run(
        flow_to_wait,
        stream_states=True,
        stream_logs=True,
        raise_final_state=True,
        max_duration=timedelta(seconds=90),
    )
