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
import time
from datetime import datetime, timedelta
from pathlib import Path

import prefect
from google.cloud import bigquery
from prefect.engine.signals import FAIL
from prefect.tasks.prefect import create_flow_run
from prefeitura_rio.pipelines_utils.logging import log

from pipelines.datalake.extract_load.vitacare_api.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_api.utils.data_transformation import (
    fix_payload_column_order,
    from_json_to_csv,
)
from pipelines.datalake.utils.data_transformations import convert_str_to_date
from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.tasks import (
    add_load_date_column,
    cloud_function_request,
    get_secret_key,
    load_file_from_bigquery,
    save_to_file,
)


@task(max_retries=4, retry_delay=timedelta(minutes=3))
def extract_data_from_api(
    cnes: str, ap: str, target_day: str, endpoint: str, environment: str = "dev"
) -> dict:
    """
    Extracts data from an API based on the provided parameters.

    Args:
        cnes (str): The CNES (National Register of Health Establishments) code.
        ap (str): The AP (Administrative Point) code.
        target_day (str): The target day for data extraction. Format YYYY-MM-DD.
        endpoint (str): The API endpoint to extract data from.
        environment (str, optional): The environment to run the extraction in. Defaults to "dev".

    Returns:
        dict: The extracted data from the API.

    Raises:
        ValueError: If the API response status code is not 200.
        ENDRUN: If the API response is empty.

    """
    api_url = vitacare_constants.BASE_URL.value[ap]
    endpoint_url = vitacare_constants.ENDPOINT.value[endpoint]

    # EXTRACT DATA

    logger = prefect.context.get("logger")
    logger.info(f"Extracting data for {endpoint} on {target_day}")

    username = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_USERNAME.value,
        environment=environment,
    )
    password = get_secret_key.run(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_VITACARE_PASSWORD.value,
        environment=environment,
    )

    response = cloud_function_request.run(
        url=f"{api_url}{endpoint_url}",
        request_type="GET",
        query_params={"date": str(target_day), "cnes": cnes},
        credential={"username": username, "password": password},
        env=environment,
    )

    if response["status_code"] != 200:
        raise ValueError(
            f"Failed to extract data from API: {response['status_code']} - {response['body']}"
        )

    requested_data = json.loads(response["body"])

    if len(requested_data) > 0:
        logger.info(f"Successful Request: retrieved {len(requested_data)} records")
        return {"data": requested_data, "has_data": True}

    else:
        target_day = datetime.strptime(target_day, "%Y-%m-%d").date()
        if (
            target_day.weekday() == 6 and endpoint == "movimento"
        ):  # There is no stock movement on Sundays because the healthcenter is closed
            logger.info("No data was retrieved. This is normal on Sundays as no data is expected.")
            return {"has_data": False}

        logger.error("Failed Request: no data was retrieved")
        raise FAIL(message=f"Empty response for ({cnes}, {target_day}, {endpoint})")


@task
def save_data_to_file(
    data: str,
    file_folder: str,
    table_id: str,
    ap: str,
    cnes: str,
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

    file_name = f"{table_id}_{ap.lower()}_cnes{cnes}"

    target_date = convert_str_to_date(load_date)

    file_path = save_to_file.run(
        data=data,
        file_folder=file_folder,
        file_name=file_name,
        add_load_date_to_filename=add_load_date_to_filename,
        load_date=target_date,
    )

    return file_path


@task
def transform_data(file_path: str, table_id: str) -> str:
    """
    Transforms data from a JSON file to a CSV file and performs additional transformations.

    Args:
        file_path (str): The path to the input JSON file.
        table_id (str): The identifier of the table for which the data is being transformed.

    Returns:
        str: The path to the output CSV file.

    Raises:
        None

    Example:
        transform_data(file_path="data.json", table_id="estoque_posicao")
    """

    csv_file_path = from_json_to_csv(input_path=file_path, sep=";")

    add_load_date_column.run(input_path=csv_file_path, sep=";")

    fix_payload_column_order(filepath=csv_file_path, table_id=table_id)

    return csv_file_path


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


# ==============================
# SCHEDULER TASKS
# ==============================
@task
def create_parameter_list(
    endpoint: str, target_date: str, dataset_id: str, table_id: str, environment: str = "dev"
):
    """
    Create a list of parameters for running the Vitacare flow.

    Args:
        environment (str, optional): The environment to run the flow in. Defaults to "dev".

    Returns:
        list: A list of dictionaries containing the flow run parameters.
    """
    # Access the health units information from BigQuery table
    dados_mestres = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name="saude_dados_mestres",
        table_name="estabelecimento",
    )
    # Filter the units using Vitacare
    unidades_vitacare = dados_mestres[
        (dados_mestres["prontuario_versao"] == "vitacare")
        & (dados_mestres["prontuario_estoque_tem_dado"] == "sim")
    ]

    # Get their CNES list
    cnes_vitacare = unidades_vitacare["id_cnes"].tolist()

    # Get the date of yesterday
    target_date = convert_str_to_date(target_date)

    # Construct the parameters for the flow
    vitacare_flow_parameters = []
    for cnes in cnes_vitacare:
        vitacare_flow_parameters.append(
            {
                "cnes": cnes,
                "endpoint": endpoint,
                "target_date": target_date,
                "dataset_id": dataset_id,
                "table_id": table_id,
                "environment": environment,
                "rename_flow": True,
                "reprocess_mode": False,
            }
        )

    logger = prefect.context.get("logger")
    logger.info(f"Created {len(vitacare_flow_parameters)} flow run parameters")

    return vitacare_flow_parameters


# ==============================
# REPROCESS TASKS
# ==============================


@task
def retrieve_cases_to_reprocessed_from_birgquery(
    dataset_id: str, table_id: str, query_limit: None
) -> list:
    """
    Retrieves cases to be reprocessed from BigQuery.

    Returns:
        list: A list of dictionaries representing the retrieved rows from BigQuery.
    """
    # Define your BigQuery client
    client = bigquery.Client()

    # Specify your dataset and table
    dataset_controle = "controle_reprocessamento"
    table_id = f"{dataset_id}__{table_id}"
    full_table_id = f"{client.project}.{dataset_controle}.{table_id}"

    if query_limit:
        retrieve_query = f"SELECT * FROM `{full_table_id}` WHERE reprocessing_status = 'pending' ORDER BY data DESC LIMIT {query_limit} "  # noqa: E501
    else:
        retrieve_query = f"SELECT * FROM `{full_table_id}` WHERE reprocessing_status = 'pending' ORDER BY data DESC"  # noqa: E501

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
        "reprocess_mode": True,
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

    if environment == "dev":
        project_name = "staging"
    elif environment == "prod":
        project_name = "production"

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
            project_name=project_name,
            parameters=params,
            run_name=f"REPROCESS: {table_id}__ap_{run['area_programatica']}.cnes_{run['id_cnes']}__{run['data'].strftime('%Y-%m-%d')}",  # noqa: E501
            idempotency_key=idempotency_key,
            scheduled_start_time=start_time + timedelta(minutes=2 * count),
        )

        time.sleep(0.5)

        parallel_runs_counter += 1

        if parallel_runs_counter == parallel_runs:
            parallel_runs_counter = 0
            count += 1


@task(max_retries=5, retry_delay=timedelta(seconds=5))
def write_on_bq_on_table(
    response: dict, dataset_id: str, table_id: str, ap: str, cnes: str, data: str
):
    """
    Writes the response data to a BigQuery table.

    Args:
        response (dict): The response data from the API.
        dataset_id (str): The ID of the BigQuery dataset.
        table_id (str): The ID of the BigQuery table.
        ap (str): The area programatica.
        cnes (str): The ID of the CNES.
        data (str): The date of the data.

    Returns:
        None
    """
    log(f"Writing response to BigQuery for {cnes} - {ap} - {data}")
    # Define your BigQuery client
    client = bigquery.Client()

    # Specify your dataset and table
    dataset_controle = "controle_reprocessamento"
    table_id = f"{dataset_id}__{table_id}"
    full_table_id = f"{client.project}.{dataset_controle}.{table_id}"

    record_to_update = {
        "id_cnes": cnes,
        "area_programatica": ap,
        "data": data,
        "reprocessing_status": "success" if response["status_code"] == 200 else "failed",
        "request_response_code": response["status_code"],
        "request_row_count": (
            len(json.loads(response["body"])) if response["status_code"] == 200 else 0
        ),
    }

    log(f"Record to update: {record_to_update}")

    # Construct the query
    record_str = (
        "STRUCT<id_cnes STRING, area_programatica STRING, data DATE, reprocessing_status STRING, request_response_code STRING, request_row_count INT64>("  # noqa: E501
        + ", ".join(
            [
                f"'{record_to_update['id_cnes']}'",
                f"'{record_to_update['area_programatica']}'",
                f"'{record_to_update['data']}'",
                f"'{record_to_update['reprocessing_status']}'",
                f"'{record_to_update['request_response_code']}'",
                str(record_to_update["request_row_count"]),
            ]
        )
        + ")"
    )
    merge_query = f"""
        MERGE `{full_table_id}` T
        USING (SELECT * FROM UNNEST([{record_str}])) S
        ON T.id_cnes = S.id_cnes AND T.data = S.data
        WHEN MATCHED THEN
        UPDATE SET
            T.reprocessing_status = S.reprocessing_status,
            T.request_response_code = S.request_response_code,
            T.request_row_count = S.request_row_count
        WHEN NOT MATCHED THEN
        INSERT (id_cnes, area_programatica, data, reprocessing_status, request_response_code, request_row_count)
        VALUES(id_cnes, area_programatica, data, reprocessing_status, request_response_code, request_row_count)
        """  # noqa: E501

    # Run the query
    query_job = client.query(merge_query)
    query_job.result()  # Wait for the job to complete

    print("Upsert operation completed.")
