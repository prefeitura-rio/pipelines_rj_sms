from pipelines.utils.credential_injector import authenticated_task as task


@task
def clone_bigquery_table(table_id, project_name):
    return