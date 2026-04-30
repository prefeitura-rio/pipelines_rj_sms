
# -- coding: utf-8 --
"""
Flow
"""
# Prefect
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
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
    name="SMS: SISREG-PREPAROS",
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


sisreg_preparos_flow.executor = LocalDaskExecutor(num_workers=16)
sisreg_preparos_flow.storage = GCS(pipeline_constants.GCS_FLOWS_BUCKET.value)
sisreg_preparos_flow.run_config = VertexRun(
    image=pipeline_constants.DOCKER_VERTEX_IMAGE.value,
    labels=[pipeline_constants.RJ_SMS_VERTEX_AGENT_LABEL.value],
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": pipeline_constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": pipeline_constants.INFISICAL_TOKEN.value,
    },
)

sisreg_preparos_flow.schedule = None
