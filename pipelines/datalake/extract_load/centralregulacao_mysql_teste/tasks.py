from pipelines.utils.credential_injector import authenticated_task as task

@task
def dummy(
    environment: str,
    host: str,
    database: str,
    port: str,
    table: str,
    query: str,
    bq_dataset: str,
):
    print(environment, host, database, port, table, query, bq_dataset)
