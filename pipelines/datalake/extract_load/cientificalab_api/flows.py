# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.cientificalab_api.tasks import (
    authenticate_and_fetch,
    transform,
)
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake
from pipelines.utils.time import get_datetime_working_range
from pipelines.datalake.extract_load.cientificalab_api.schedules import schedule


with Flow(
    name="DataLake - Extração e Carga de Dados - CientificaLab",
) as flow_cientificalab:
    ENVIRONMENT = Parameter("environment", default="dev")
    DT_INICIO = Parameter("dt_inicio", default="")
    DT_FIM = Parameter("dt_fim", default="")

    username_secret = get_secret_key(
        secret_path="/cientificalab", secret_name="USERNAME", environment=ENVIRONMENT
    )
    password_secret = get_secret_key(
        secret_path="/cientificalab", secret_name="PASSWORD", environment=ENVIRONMENT
    )
    apccodigo_secret = get_secret_key(
        secret_path="/cientificalab", secret_name="APCCODIGO", environment=ENVIRONMENT
    )

    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=DT_INICIO,
        end_datetime=DT_FIM,
        interval=1,
        return_as_str=True,
    )

    resultado_xml = authenticate_and_fetch(
        username=username_secret,
        password=password_secret,
        apccodigo=apccodigo_secret,
        dt_inicio=start_datetime,
        dt_fim=end_datetime,
    )

    solicitacoes_df, exames_df, resultados_df = transform(resultado_xml=resultado_xml)

    solicitacoes_upload_task = upload_df_to_datalake(
        df=solicitacoes_df,
        dataset_id="brutos_cientificalab",
        table_id="solicitacoes",
        source_format="parquet",
        partition_column="datalake_loaded_at",
    )
    exames_upload_task = upload_df_to_datalake(
        df=exames_df,
        dataset_id="brutos_cientificalab",
        table_id="exames",
        source_format="parquet",
        partition_column="datalake_loaded_at",
        upstream_tasks=[solicitacoes_upload_task],
    )
    resultados_upload_task = upload_df_to_datalake(
        df=resultados_df,
        dataset_id="brutos_cientificalab",
        table_id="resultados",
        source_format="parquet",
        partition_column="datalake_loaded_at",
        upstream_tasks=[exames_upload_task],
    )

flow_cientificalab.schedule = schedule
flow_cientificalab.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cientificalab.executor = LocalDaskExecutor(num_workers=3)
flow_cientificalab.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="4Gi",
)
