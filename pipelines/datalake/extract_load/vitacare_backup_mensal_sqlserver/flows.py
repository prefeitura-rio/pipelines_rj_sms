# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow de extração e carga de dados do Vitacare Historic SQL Server para o BigQuery
"""
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow # Importa seu Flow customizado

from pipelines.constants import constants
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake 
from pipelines.datalake.utils.tasks import rename_current_flow_run

from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.tasks import (
    get_sql_server_engine,
    extract_and_transform_table,
    build_bq_table_name,
)

from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.schedules import (
    vitacare_monthly_schedule, 
)

with Flow("DataLake - Extração e Carga - Vitacare Historic") as flow_vitacare_historic:
    # Parâmetros do Flow
    ENVIRONMENT = Parameter("environment", default="prod")
    SCHEMA = Parameter("schema", default="dbo")
    PARTITION_COLUMN = Parameter("partition_column", default="extracted_at")
    RENAME_FLOW = Parameter("rename_flow", default=False) #

    CNES_CODE_PARAM = Parameter("cnes_code", required=True)


    DATASET_ID = vitacare_constants.DATASET_ID.value
    TABLES_TO_EXTRACT = vitacare_constants.TABLES_TO_EXTRACT.value
    DB_NAME = f"vitacare_historic_{CNES_CODE_PARAM}" # O nome do banco de dados depende do CNES


    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            dataset=DATASET_ID,
            table=f"vitacare_all_tables_{CNES_CODE_PARAM}",
        )


    host = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_HOST.value,
        environment=ENVIRONMENT,
    )
    port = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_PORT.value,
        environment=ENVIRONMENT,
    )
    user = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_USERNAME.value,
        environment=ENVIRONMENT,
    )
    password = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_PASSWORD.value,
        environment=ENVIRONMENT,
    )

    sql_server_engine = get_sql_server_engine(
        db_host=host,
        db_port=port,
        db_user=user,
        db_password=password,
        db_name=DB_NAME,
    )


    for table_name in TABLES_TO_EXTRACT:

        extracted_df = extract_and_transform_table(
            db_url=sql_server_engine,
            db_schema=SCHEMA,
            db_table=table_name,
            cnes_code=CNES_CODE_PARAM, # Usa o parâmetro do flow para o CNES
        )


        bq_table_id = build_bq_table_name(table_name)


        upload_df_to_datalake(
            df=extracted_df,
            dataset_id=DATASET_ID,
            table_id=bq_table_id,
            partition_column=PARTITION_COLUMN,
            source_format="parquet", 
            if_exists="append", 
            if_storage_data_exists="append", 
        )


flow_vitacare_historic.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic.executor = LocalDaskExecutor(num_workers=10) 
flow_vitacare_historic.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="8Gi", 
    memory_request="5Gi",
)
flow_vitacare_historic.schedule = vitacare_monthly_schedule