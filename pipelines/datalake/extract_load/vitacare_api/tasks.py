# -*- coding: utf-8 -*-
# pylint: disable= C0301. C0103
# flake8: noqa: E501
"""
Tasks for dump_api_vitacare
"""

import json
import os
import re
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

import prefect
from google.cloud import bigquery
from prefect.engine.signals import FAIL
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


@task(max_retries=2, retry_delay=timedelta(minutes=2))
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
        dict: The extracted data from the API

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
        endpoint_for_filename=endpoint,
    )

    if response["status_code"] != 200:
        raise FAIL(
            f"Failed to extract data from API: {response['status_code']} - {response['body']}"
        )

    requested_data = response["body"]

    if len(requested_data) > 0:

        # check if the data was replicated today. This is exclusive to the endpoint "posicao"
        if endpoint == "posicao":

            replication_datetime = datetime.strptime(
                requested_data[0]["dtaReplicacao"], "%Y-%m-%d %H:%M:%S.%f"
            )

            yesterday_cutoff = (datetime.now() - timedelta(days=1)).replace(
                hour=20, minute=0, second=0, microsecond=0
            )

            if replication_datetime < yesterday_cutoff:
                if cnes in (
                    "6927254",
                    "7873565",
                ):  # TODO: remove this condition after Newton Bethlem internet is fixed
                    target_day = replication_datetime.strftime("%Y-%m-%d")
                else:
                    err_msg = (
                        f"API data is outdated. "
                        f"Last update at API: {replication_datetime}, "
                        f"Expected update after: {yesterday_cutoff}. "
                    )
                    logger.error(err_msg)
                    return {"has_data": False}

        logger.info(f"Successful Request: retrieved {len(requested_data)} records")
        return {
            "data": requested_data,
            "has_data": True,
            "replication_date": target_day,
        }

    else:
        target_day = datetime.strptime(target_day, "%Y-%m-%d").date()
        if endpoint in ("movimento", "vacina") and (
            target_day.weekday() == 6
            or prefect.context.task_run_count >= 2  # pylint: disable=no-member
        ):
            logger.info("No data was retrieved for the target day")
            return {"has_data": False}

        logger.error("Failed Request: no data was retrieved")
        raise FAIL(f"Empty response for ({cnes}, {target_day}, {endpoint})")


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

    if table_id in ("estoque_posicao", "estoque_movimento", "vacina"):
        fix_payload_column_order(filepath=csv_file_path, table_id=table_id)

    return csv_file_path


@task
def create_partitions(data_path: List[str] | str, partition_directory: str, file_type="csv"):
    """
    Create partitions for the given data files and copy them to the specified partition directory.

    Args:
        data_path (str): The path to the data file(s) or directory containing the data files.
        partition_directory (str): The directory where the partitions will be created.
        file_type (str, optional): The file type of the data files. Defaults to "csv".

    Raises:
        ValueError: If the filename does not contain a date in the format YYYY-MM-DD.

    Returns:
        None
    """
    # check if data_path is a directory or a file
    if isinstance(data_path, str):
        if os.path.isdir(data_path):
            data_path = Path(data_path)
            files = data_path.glob(f"*.{file_type}")
        else:
            files = [data_path]
    elif isinstance(data_path, list):
        files = data_path
    else:
        raise ValueError("data_path must be a string or a list")

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
    endpoint: str,
    target_date: str,
    dataset_id: str,
    table_id: str,
    environment: str = "dev",
    area_programatica: str = None,
    is_routine: bool = True,
):
    """
    Create a list of parameters for running the Vitacare flow.

    Args:
        environment (str, optional): The environment to run the flow in. Defaults to "dev".

    Returns:
        list: A list of dictionaries containing the flow run parameters.
    """
    # Access the health units information from BigQuery table
    if is_routine:
        dataset_name = "saude_dados_mestres"
        table_name = "estabelecimento"
    else:
        dataset_name = "gerenciamento__reprocessamento"
        if endpoint == "movimento":
            table_name = "brutos_prontuario_vitacare__estoque_movimento"
        elif endpoint == "vacina":
            table_name = "brutos_prontuario_vitacare__vacina"
        else:
            log("Invalid endpoint", level="error")
            raise FAIL("Invalid endpoint")

    table_data = load_file_from_bigquery.run(
        project_name="rj-sms",
        dataset_name=dataset_name,
        table_name=table_name,
    )

    # Filter the queried table
    if is_routine:

        if endpoint in ["backup_prontuario", "vacina"]:
            results = table_data[
                (table_data["prontuario_versao"] == "vitacare")
                & (table_data["prontuario_episodio_tem_dado"] == "sim")
            ]
        elif endpoint in ["posicao", "movimento"]:
            results = table_data[
                (table_data["prontuario_versao"] == "vitacare")
                & (table_data["prontuario_estoque_tem_dado"] == "sim")
            ]
        else:
            log("Invalid endpoint", level="error")
            raise FAIL("Invalid endpoint")

        if endpoint in ["movimento", "posicao", "vacina"]:
            results["data"] = convert_str_to_date(target_date)

    else:
        # retrieve records with pending or in progress retry status
        results = table_data[
            (table_data["retry_status"] == "pending")
            | (table_data["retry_status"] == "in progress")
        ]

        data_limite = (datetime.now() - timedelta(days=7)).date()
        results = results[results["data"] >= data_limite]

        results["data"] = results.data.apply(lambda x: x.strftime("%Y-%m-%d"))

    if area_programatica:
        results = results[results["area_programatica"] == area_programatica]

    if results.empty and is_routine:
        log("No data to process", level="error")
        raise FAIL("No data to process")

    # Construct the parameters for the flow

    vitacare_flow_parameters = []

    if endpoint in ["movimento", "posicao", "vacina"]:

        results_tuples = results[["id_cnes", "data"]].apply(tuple, axis=1).tolist()

        for cnes, date_target in results_tuples:
            vitacare_flow_parameters.append(
                {
                    "cnes": cnes,
                    "endpoint": endpoint,
                    "target_date": date_target,
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "environment": environment,
                    "rename_flow": True,
                    "is_routine": is_routine,
                }
            )

    elif endpoint == "backup_prontuario":
        for cnes in results["id_cnes"]:
            vitacare_flow_parameters.append(
                {
                    "environment": environment,
                    "rename_flow": True,
                    "cnes": cnes,
                    "backup_subfolder": target_date,
                    "dataset_id": dataset_id,
                    "upload_only_expected_files": True,
                }
            )

    else:
        log("Invalid endpoint", level="error")
        raise FAIL("Invalid endpoint")

    logger = prefect.context.get("logger")
    logger.info(f"Created {len(vitacare_flow_parameters)} flow run parameters")

    return vitacare_flow_parameters


# ==============================
# REPROCESS TASKS
# ==============================


@task(max_retries=5, retry_delay=timedelta(seconds=5))
def write_retry_results_on_bq(
    endpoint: str, response: dict, ap: str, cnes: str, target_date: str, max_retries: int = 2
):
    """
    Writes the retry results to BigQuery.

    Args:
        endpoint (str): The endpoint for the API.
        response (dict): The response from the API.
        ap (str): The area programatica.
        cnes (str): The CNES.
        target_date (str): The target date.
        max_retries (int, optional): The maximum number of retries. Defaults to 2.
    """

    # Define your BigQuery client
    client = bigquery.Client()

    # Specify your dataset and table
    dataset_id = "gerenciamento__reprocessamento"
    if endpoint == "movimento":
        table_id = "brutos_prontuario_vitacare__estoque_movimento"
    elif endpoint == "vacina":
        table_id = "brutos_prontuario_vitacare__vacina"
    else:
        err_msg = "Invalid endpoint"
        log(err_msg, level="error")
        raise FAIL(err_msg)

    full_table_id = f"{client.project}.{dataset_id}.{table_id}"

    # retrieve the data to be updated from the table
    retrieve_query = f"""
        SELECT *
        FROM `{full_table_id}`
        WHERE id_cnes = '{cnes}' AND data = '{target_date}'
        """

    query_job = client.query(retrieve_query)
    results = query_job.result()

    if results.total_rows == 1:
        results_list = []
        for row in results:
            result_dict = {
                "id_cnes": row["id_cnes"],
                "area_programatica": row["area_programatica"],
                "data": row["data"],
                "nome_limpo": row["nome_limpo"],
                "retry_status": row["retry_status"],
                "retry_attempts_count": row["retry_attempts_count"],
                "request_row_count": row["request_row_count"],
            }
            results_list.append(result_dict)
        previous_record = results_list[0]
    else:
        err_msg = f"Records found: {results.total_rows}. Expected 1."
        log(err_msg, level="error")
        raise FAIL(err_msg)

    # Update the retry status and retry attempts count
    retry_attempts_count = previous_record["retry_attempts_count"] + 1

    record_to_update = {
        "id_cnes": cnes,
        "area_programatica": ap,
        "data": target_date,
        "nome_limpo": previous_record["nome_limpo"],
        "retry_status": (
            "finished"
            if response["has_data"] is True or retry_attempts_count >= max_retries
            else "in progress"
        ),
        "retry_attempts_count": retry_attempts_count,
        "request_row_count": (len(response["data"]) if response["has_data"] is True else 0),
    }

    log(f"Record to update: {record_to_update}", level="debug")

    # Construct the query
    record_str = (
        "STRUCT<id_cnes STRING, area_programatica STRING, data DATE, nome_limpo STRING, retry_status STRING, retry_attempts_count INT64, request_row_count INT64>("  # noqa: E501
        + ", ".join(
            [
                f"'{record_to_update['id_cnes']}'",
                f"'{record_to_update['area_programatica']}'",
                f"'{record_to_update['data']}'",
                f"'{record_to_update['nome_limpo']}'",
                f"'{record_to_update['retry_status']}'",
                str(record_to_update["retry_attempts_count"]),
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
            T.retry_status = S.retry_status,
            T.retry_attempts_count = S.retry_attempts_count,
            T.request_row_count = S.request_row_count
        WHEN NOT MATCHED THEN
        INSERT (id_cnes, area_programatica, data, nome_limpo, retry_status, retry_attempts_count, request_row_count)
        VALUES(id_cnes, area_programatica, data, nome_limpo, retry_status, retry_attempts_count, request_row_count)
        """  # noqa: E501

    # Run the query
    query_job = client.query(merge_query)
    query_job.result()  # Wait for the job to complete

    log("Record updated successfully", level="info")


@task
def get_flow_name(endpoint: str):
    """
    Get the flow name based on the endpoint

    Args:
        endpoint (str): The endpoint for the API.

    Returns:
        str: The flow name.
    """
    if endpoint in ["movimento", "posicao", "vacina"]:
        flow_name = "DataLake - Extração e Carga de Dados - VitaCare"
    elif endpoint == "backup_prontuario":
        flow_name = "DataLake - Extração e Carga de Dados - VitaCare DB"
    else:
        err_msg = "Invalid endpoint. Expected 'movimento', 'posicao', 'vacina' or 'backup_prontuario'"  # noqa: E501
        log(err_msg, level="error")
        raise FAIL(err_msg)
    return flow_name
