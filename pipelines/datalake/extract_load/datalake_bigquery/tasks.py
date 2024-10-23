from google.cloud import bigquery

from pipelines.utils.credential_injector import authenticated_task as task
from pipelines.utils.logger import log

@task
def clone_bigquery_table(
    source_table_id, 
    destination_table_name, 
    destination_dataset_name,
    destination_project_name
):
    bq_client = bigquery.Client.from_service_account_json("/tmp/credentials.json")

    destination_table_id = f"{destination_project_name}.{destination_dataset_name}.{destination_table_name}" # noqa

    command = f"CREATE OR REPLACE TABLE `{destination_table_id}` CLONE `{source_table_id}`"
    log(f"Running: {command}")

    query_job = bq_client.query_and_wait(command)
    result = query_job.result()
    log(f"Query result: {result}")

    return result