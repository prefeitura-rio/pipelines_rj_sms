# -*- coding: utf-8 -*-

from typing import List

import google
import google.api_core
from google.cloud import bigquery
from prefect.engine.signals import FAIL

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def clone_bigquery_table(
    source_project_name: str,
    source_dataset_name: str,
    source_table_list: List[str],
    destination_project_name: str,
    destination_dataset_name: str,
):
    """
    Clones tables from a source BigQuery dataset to a destination dataset within BigQuery.
    Creates the destination dataset if it does not exist.
    In case of an error during cloning, attempts to clone by copying data instead.

    Args:
        source_project_name (str): The name of the source project in BigQuery.
        source_dataset_name (str): The name of the source dataset in BigQuery.
        source_table_list (List[str]): List of table names to be cloned from the source dataset.
        destination_project_name (str): The name of the destination project in BigQuery.
        destination_dataset_name (str): The name of the destination dataset in BigQuery.

    Returns:
        str: The state of the job after cloning the table.
    """
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

    for table in source_table_list:

        source_table_id = f"{source_project_name}.{source_dataset_name}.{table}"
        destination_dataset_id = f"{destination_project_name}.{destination_dataset_name}"
        destination_table_id = f"{destination_dataset_id}.{table}"

        bq_client.create_dataset(destination_dataset_id, exists_ok=True)

        log(f"Cloning table {source_table_id} to {destination_table_id}")

        try:
            command = f"CREATE OR REPLACE TABLE `{destination_table_id}` CLONE `{source_table_id}`"
            log(f"Running: {command}")
            query_job = bq_client.query_and_wait(command)
            job = bq_client.get_job(query_job.job_id)
            log(f"Result: {job.state}")

        except google.api_core.exceptions.BadRequest as e:
            log(f"Clone method failed: {e}", level="warning")

            log("Trying to clone table by copying data from source to destination")

            command = f"DROP TABLE IF EXISTS `{destination_table_id}`;"
            log(f"Running: {command}")
            query_job = bq_client.query_and_wait(command)
            job = bq_client.get_job(query_job.job_id)
            log(f"Result: {job.state}")

            command = f"CREATE TABLE `{destination_table_id}` AS SELECT * FROM `{source_table_id}`"
            log(f"Running: {command}")
            query_job = bq_client.query_and_wait(command)
            job = bq_client.get_job(query_job.job_id)
            log(f"Result: {job.state}")

        except Exception as e:
            log(f"Unexpected error: {e}", level="error")
            raise FAIL(f"Unexpected error: {e}") from e
