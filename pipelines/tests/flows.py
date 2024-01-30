from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.utils.tasks import (
    inject_gcp_credentials
)
from pipelines.tests.tasks import (
    list_all_secrets_name
)


with Flow(
    name="Teste de Ambiente",
) as test_ambiente:
    
    list_all_secrets_name(environment='dev')

    inject_gcp_credentials(environment='dev')
