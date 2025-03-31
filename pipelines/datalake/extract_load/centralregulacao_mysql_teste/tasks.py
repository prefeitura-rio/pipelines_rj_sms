from pipelines.utils.credential_injector import authenticated_task as task

@task
def dummy(
    environment: str,
    name: str,
):
    print(environment, name)
