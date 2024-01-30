from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.utils.tasks import (
    inject_gcp_credentials
)
from pipelines.tests.tasks import (
    list_all_secrets_name
)
from prefect import Parameter


with Flow(
    name="Teste de Ambiente",
) as test_ambiente:
    
    ENVIRONMENT = Parameter("environment", default="dev")
    
    list_all_secrets_name(environment=ENVIRONMENT)

    inject_gcp_credentials(environment=ENVIRONMENT)
