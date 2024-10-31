# -*- coding: utf-8 -*-
import google
import google.api_core
from google.cloud import bigquery
from prefect.engine.signals import ENDRUN

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log


@task
def clone_bigquery_table(
    source_table_id, destination_table_name, destination_dataset_name, destination_project_name
):
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

    destination_dataset_id = f"{destination_project_name}.{destination_dataset_name}"
    destination_table_id = f"{destination_dataset_id}.{destination_table_name}"

    bq_client.create_dataset(destination_dataset_id, exists_ok=True)

    try:
        command = f"CREATE OR REPLACE TABLE `{destination_table_id}` CLONE `{source_table_id}`"
        log(f"Running: {command}")
        query_job = bq_client.query_and_wait(command)
        job = bq_client.get_job(query_job.job_id)
        log(f"Result: {job.state}")
        return job.state

    except google.api_core.exceptions.BadRequest as e:

        log(f"Clone method failed: {e}", level="warning")

        log("Trying to clone table by copying data from source to destination")

        command = f"DROP TABLE IF EXISTS `{destination_table_id}`; CREATE TABLE `{destination_table_id}` AS SELECT * FROM `{source_table_id}`"
        log(f"Running: {command}")
        query_job = bq_client.query_and_wait(command)
        job = bq_client.get_job(query_job.job_id)
        log(f"Result: {job.state}")
        return job.state

    except google.api_core.exceptions.Forbidden as e:
        service_account = bq_client._credentials.service_account_email
        msg = f"{service_account} has not enough permition to clone table {source_table_id}: {e}"
        log(msg, level="error")
        raise ENDRUN(msg)
