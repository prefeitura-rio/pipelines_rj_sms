
# -- coding: utf-8 --
"""
Flow
"""
# Prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.constants import constants as pipeline_constants

# Internos
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.datalake.extract_load.sisreg_preparos import constants
from pipelines.datalake.extract_load.sisreg_preparos.tasks import (
    login,
    coletar_unidades,
    processar_unidades
)

from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake


with Flow(
    name="SMS: SISREG-PREPAROS_",
    state_handlers=[handle_flow_state_change],
) as sisreg_preparos_flow:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)

    # Secrets
    usuario = get_secret_key(
        secret_path=constants.INFISICAL_SISREG_PATH,
        secret_name=constants.INFISICAL_SISREG_USERNAME,
        environment=ENVIRONMENT,
    )

    senha = get_secret_key(
        secret_path=constants.INFISICAL_SISREG_PATH,
        secret_name=constants.INFISICAL_SISREG_PASSWORD,
        environment=ENVIRONMENT,
    )

    # Fluxo
    session = login(user=usuario, senha=senha)
    unidades = coletar_unidades(session)
    df_preparos = processar_unidades(session, unidades)
    
    df_preparos_ajustados = handle_columns_to_bq(
                            df=df_preparos[['cnes',
                                            'nomeUnidade',
                                            'codProcedimento',
                                            'nomeProcedimento',
                                            'descricaoPreparo']])

    upload_df_to_datalake(
        df=df_preparos_ajustados,
        dataset_id="brutos_sisreg_preparos",
        table_id="tb_sisreg_preparos",
        partition_column="dt_extracao",
        source_format="parquet",
    )


sisreg_preparos_flow.executor = LocalDaskExecutor(num_workers=1)
sisreg_preparos_flow.storage = GCS(
    pipeline_constants.GCS_FLOWS_BUCKET.value
)

sisreg_preparos_flow.run_config = KubernetesRun(
    image=pipeline_constants.DOCKER_IMAGE.value,
    labels=[pipeline_constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="10Gi",
    memory_limit="10Gi",
)