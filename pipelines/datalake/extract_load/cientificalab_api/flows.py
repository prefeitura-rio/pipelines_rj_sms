from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import constants

from pipelines.utils.tasks import (
    upload_df_to_datalake,
    get_secret_key,
)

from pipelines.datalake.extract_load.cientificalab_api.tasks import (
    authenticate_and_fetch,
    transform,
)

with Flow(
    name="DataLake - Extração e Carga de Dados - CientíficaLab",
) as flow:

    ENVIRONMENT = Parameter("environment", default="dev")
    DT_INICIO = Parameter("dt_inicio", required=True)
    DT_FIM = Parameter("dt_fim", required=True)

    username_secret = get_secret_key(
        secret_path="/cientificalab",
        secret_name="USERNAME",
        environment=ENVIRONMENT
    )
    password_secret = get_secret_key(
        secret_path="/cientificalab",
        secret_name="PASSWORD",
        environment=ENVIRONMENT
    )
    apccodigo_secret = get_secret_key(
        secret_path="/cientificalab",
        secret_name="APCCODIGO",
        environment=ENVIRONMENT
    )

    resultado_xml = authenticate_and_fetch(
        username=username_secret,
        password=password_secret,
        apccodigo=apccodigo_secret,
        dt_inicio=DT_INICIO,
        dt_fim=DT_FIM
    )

    solicitacoes_df, exames_df, resultados_df = transform(resultado_xml)

    upload_df_to_datalake(solicitacoes_df, 'solicitacoes')
    upload_df_to_datalake(exames_df, 'exames')
    upload_df_to_datalake(resultados_df, 'resultados')

flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.executor = LocalDaskExecutor(num_workers=10)
flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)