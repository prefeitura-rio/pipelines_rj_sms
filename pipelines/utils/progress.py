# -*- coding: utf-8 -*-
# FLOW PATTERN: Manager-Operator

# Flow Operário
# - Recebe parâmetros, executa a task e salva o progresso

# Flow Gerente
# - Cria lista de parâmetros ainda não executados
# - Cria execuções de Flow Operário

import datetime

import pandas as pd
import prefect
from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.googleutils import generate_bigquery_schema
from pipelines.utils.logger import log


@task
def calculate_operator_key(exclude_keys=[], **kwargs):
    """
    Calculate the operator key based on the given arguments.
    Parameters:
    exclude_keys (list): A list of keys to exclude from the calculation.
    **kwargs: Additional keyword arguments.
    Returns:
    str: The operator key calculated based on the given arguments.
    """
    all_args = [(key, value) for key, value in kwargs.items() if key not in exclude_keys]

    args_sorted_by_key = sorted(all_args, key=lambda x: x[0])

    key_pieces = [str(value) for _, value in args_sorted_by_key]

    return ".".join(key_pieces)


@task
def load_operators_progress(slug, project_name="rj-sms-dev"):
    """
    Load the progress data for a given slug from BigQuery table.

    Args:
        slug (str): The slug of the progress data.
        project_name (str, optional): The name of the project. Defaults to "rj-sms-dev".

    Returns:
        pandas.DataFrame or None: The progress data as a pandas DataFrame if the table exists,
        otherwise None.
    """
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

    table_name = f"{project_name}.gerenciamento__progresso.{slug}"

    try:
        bq_client.get_table(table_name)
    except Exception:
        log("Table not found")
        return None

    query = f"""SELECT * FROM {table_name}"""
    all_records = bq_client.query(query).result().to_dataframe()
    return all_records


@task
def save_operator_progress(operator_key, slug, project_name="rj-sms-dev"):
    """
    Saves the progress of an operator in a BigQuery table.
    Args:
        operator_key (str): The key of the operator.
        slug (str): The slug of the table.
        project_name (str, optional): The name of the project. Defaults to "rj-sms-dev".
    """
    rows_to_insert = pd.DataFrame(
        [
            {
                "operator_key": operator_key,
                "flow_run_id": prefect.context.get("flow_run_id"),
                "moment": datetime.datetime.now().isoformat(),
            }
        ]
    )

    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

    dataset_name = f"{project_name}.gerenciamento__progresso"
    table_name = f"{dataset_name}.{slug}"

    log(rows_to_insert)

    bq_client.create_dataset(dataset_name, exists_ok=True)
    bq_client.create_table(table_name, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        schema=generate_bigquery_schema(rows_to_insert),
        write_disposition="WRITE_APPEND",
    )
    job = bq_client.load_table_from_dataframe(rows_to_insert, table_name, job_config=job_config)
    job.result()


@task
def get_remaining_operators(progress_table: pd.DataFrame | None, all_operators_params: list[dict]):
    """
    Retrieves the remaining operators from a list of all operators' parameters.

    Args:
        progress_table (pd.DataFrame | None): A DataFrame representing the progress table.
            If provided, the function will merge it with the candidates DataFrame.
        all_operators_params (list[dict]): A list of dictionaries representing the parameters
            of all operators.

    Returns:
        list[dict]: A list of dictionaries representing the remaining operators' parameters.
    """

    candidates = pd.DataFrame(all_operators_params)
    candidates_columns = candidates.columns.tolist()

    if progress_table is not None:
        candidates = candidates.merge(
            how="outer",
            right=progress_table,
            on=["operator_key"],
            indicator=True,
        )
        remaining = candidates[candidates["_merge"] == "left_only"][candidates_columns].to_dict(
            orient="records"
        )
    else:
        remaining = candidates[candidates_columns].to_dict(orient="records")

    return remaining
