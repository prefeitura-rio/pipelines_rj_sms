from prefect import task

from prefeitura_rio.pipelines_utils.env import getenv_or_action
from prefeitura_rio.pipelines_utils.infisical import get_infisical_client
from prefeitura_rio.pipelines_utils.logging import log


@task
def list_all_secrets_name(
    environment: str = 'dev',
    path: str = "/",
) -> None:
    token = getenv_or_action("INFISICAL_TOKEN", default=None)
    log(f"""Token: {token}""")

    client = get_infisical_client()
    secrets = client.get_all_secrets(environment=environment, path=path)

    secret_names = []
    for secret in secrets:
        secret_names.append( secret.secret_name )
    
    log(f"""Secrets in path: {", ".join(secret_names)}""")