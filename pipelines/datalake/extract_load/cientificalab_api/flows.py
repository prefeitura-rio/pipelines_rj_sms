# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.run_configs import VertexRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.cientificalab_api.constants import (
    constants as cientificalab_constants,
)
from pipelines.datalake.extract_load.cientificalab_api.schedules import schedule
from pipelines.datalake.extract_load.cientificalab_api.tasks import (
    authenticate_and_fetch,
    transform,
)
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake
from pipelines.utils.time import get_datetime_working_range

with Flow(
    name="DataLake - Extração e Carga de Dados - CientificaLab",
) as flow_cientificalab:
    ENVIRONMENT = Parameter("environment", default="dev")
    DT_INICIO = Parameter("dt_inicio", default="")
    DT_FIM = Parameter("dt_fim", default="")

    # INFISICAL
    INFISICAL_PATH = cientificalab_constants.INFISICAL_PATH.value
    INFISICAL_USERNAME = cientificalab_constants.INFISICAL_USERNAME.value
    INFISICAL_PASSWORD = cientificalab_constants.INFISICAL_PASSWORD.value
    INFISICAL_APCCODIGO = cientificalab_constants.INFISICAL_APCCODIGO.value

    username_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_USERNAME, environment=ENVIRONMENT
    )
    password_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_PASSWORD, environment=ENVIRONMENT
    )
    apccodigo_secret = get_secret_key(
        secret_path=INFISICAL_PATH, secret_name=INFISICAL_APCCODIGO, environment=ENVIRONMENT
    )

    # BIG QUERY
    DATASET_ID = Parameter(
        "dataset_id", default=cientificalab_constants.DATASET_ID.value, required=False
    )

    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=DT_INICIO,
        end_datetime=DT_FIM,
        interval=1,
        return_as_str=True,
        timezone="America/Sao_Paulo",
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
        dataset_id=DATASET_ID,
        table_id="solicitacoes",
        source_format="parquet",
        partition_column="datalake_loaded_at",
    )
    exames_upload_task = upload_df_to_datalake(
        df=exames_df,
        dataset_id=DATASET_ID,
        table_id="exames",
        source_format="parquet",
        partition_column="datalake_loaded_at",
        upstream_tasks=[solicitacoes_upload_task],
    )
    resultados_upload_task = upload_df_to_datalake(
        df=resultados_df,
        dataset_id=DATASET_ID,
        table_id="resultados",
        source_format="parquet",
        partition_column="datalake_loaded_at",
        upstream_tasks=[exames_upload_task],
    )

flow_cientificalab.schedule = schedule
flow_cientificalab.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_cientificalab.run_config = VertexRun(
    image=constants.DOCKER_VERTEX_IMAGE.value,
    labels=[constants.RJ_SMS_VERTEX_AGENT_LABEL.value],
    machine_type="e2-standard-4",  # ou outro tipo necessário
    env={
        "INFISICAL_ADDRESS": constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": constants.INFISICAL_TOKEN.value,
    },
)