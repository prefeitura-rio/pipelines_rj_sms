# -*- coding: utf-8 -*-
"""
Flow
"""
from prefect import Parameter
from prefect.storage import GCS
from pipelines.utils.flow import Flow
from prefect.run_configs import VertexRun
from prefect.executors import LocalDaskExecutor
from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.utils.tasks import handle_columns_to_bq
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

from pipelines.datalake.extract_load.sisreg_pendentes_vagas import constants, schedules
from pipelines.datalake.extract_load.sisreg_pendentes_vagas.tasks import (
    get_elasticsearch_client,
    get_procedimentos,
    initiate_session,
    extract_vagas_info
)


with Flow(
    name="SUBGERAL/CR/NTI - Extract & Load - SISREG FILA E VAGAS",
    state_handlers=[handle_flow_state_change],
    owners=[
        pipeline_constants.MATHEUS_ID.value,
    ],
) as sisreg_fila_vagas_flow:

    ENVIRONMENT = Parameter("environment", default="staging", required=True)
    DATASET_ID = Parameter("dataset_id", default=constants.DEFAULT_DATASET_ID, required=False)
    REQUEST_DELAY = Parameter("request_delay", default=constants.DEFAULT_REQUEST_DELAY, required=False)

    MAX_PROCEDIMENTOS = Parameter("max_procedimentos", default=constants.DEFAULT_MAX_PROCEDIMENTOS, required=False)
    MAX_ES_PAGES = Parameter("max_es_pages", default=constants.DEFAULT_MAX_ES_PAGES, required=False)
    
    FILA_E_VAGAS_TABLE_ID = Parameter(
        "fila_e_vagas_table_id", default=constants.DEFAULT_FILA_E_VAGAS_TABLE_ID, required=False
    )
    VAGAS_DETALHADAS_TABLE_ID = Parameter(
        "vagas_detalhadas_table_id", default=constants.DEFAULT_VAGAS_DETALHADAS_TABLE_ID, required=False
    )


    sisreg_api_user = get_secret_key(
        environment=ENVIRONMENT, 
        secret_name=constants.INFISICAL_SISREG_API_USERNAME, 
        secret_path=constants.INFISICAL_SISREG_API_PATH
    )
    sisreg_api_password = get_secret_key(
        environment=ENVIRONMENT, 
        secret_name=constants.INFISICAL_SISREG_API_PASSWORD, 
        secret_path=constants.INFISICAL_SISREG_API_PATH
    )

    sisreg_web_user = get_secret_key(
        environment=ENVIRONMENT, 
        secret_name=constants.INFISICAL_SISREG_USERNAME, 
        secret_path=constants.INFISICAL_SISREG_PATH
    )
    sisreg_web_password = get_secret_key(
        environment=ENVIRONMENT, 
        secret_name=constants.INFISICAL_SISREG_PASSWORD, 
        secret_path=constants.INFISICAL_SISREG_PATH
    )
    

    es = get_elasticsearch_client(user=sisreg_api_user, password=sisreg_api_password)
    df_procedimentos = get_procedimentos(es=es, max_es_pages=MAX_ES_PAGES)
    session = initiate_session(user=sisreg_web_user, password=sisreg_web_password, request_delay=REQUEST_DELAY)
    dfs_results_list = extract_vagas_info(
        session=session,
        df_procedimentos=df_procedimentos,
        request_delay=REQUEST_DELAY, 
        max_procedimentos=MAX_PROCEDIMENTOS
        )
    
    df_general_data, df_vagas_details = dfs_results_list[0], dfs_results_list[1]
    
    df_general_data_ok = handle_columns_to_bq(df=df_general_data)
    df_vagas_details_ok = handle_columns_to_bq(df=df_vagas_details)

    upload_df_to_datalake(
        df=df_general_data_ok,
        dataset_id=DATASET_ID,
        table_id=FILA_E_VAGAS_TABLE_ID,
        partition_column=constants.EXTRACTION_DATE_COLUMN,
        source_format="parquet",
    )

    upload_df_to_datalake(
        df=df_vagas_details_ok,
        dataset_id=DATASET_ID,
        table_id=VAGAS_DETALHADAS_TABLE_ID,
        partition_column=constants.EXTRACTION_DATE_COLUMN,
        source_format="parquet",
    )

sisreg_fila_vagas_flow.executor = LocalDaskExecutor(num_workers=1)
sisreg_fila_vagas_flow.storage = GCS(pipeline_constants.GCS_FLOWS_BUCKET.value)
sisreg_fila_vagas_flow.run_config = VertexRun(
    image=pipeline_constants.DOCKER_VERTEX_IMAGE.value,
    labels=[pipeline_constants.RJ_SMS_VERTEX_AGENT_LABEL.value],
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": pipeline_constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": pipeline_constants.INFISICAL_TOKEN.value,
    },
)
sisreg_fila_vagas_flow.schedule = schedules.schedule
