# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow de extração e carga mensal do backup do Vitacare (SQL Server → BigQuery)
"""

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.tasks import (
    get_sql_server_engine,
    get_sql_server_credentials,
    create_extraction_batches_sql_server,
    download_from_sql_server_db,
    build_bq_table_name_vitacare,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.schedules import (
    vitacare_monthly_schedule,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import upload_df_to_datalake

with Flow("DataLake - Extração e Carga - Vitacare Backup Mensal") as flow_vitacare_backup_mensal:
    # Parâmetros principais
    ENVIRONMENT = Parameter("environment", default="dev")
    CNES_CODE = Parameter("id_code", required=True)
    SCHEMA = Parameter("schema", default="dbo")
    DATASET_ID = Parameter("dataset_id", default="brutos_vitacare_historic")
    PARTITION_COLUMN = Parameter("partition_column", default=None)
    RENAME_FLOW = Parameter("rename_flow", default=False)
    ID_COLUMN = Parameter("id_column", default="id")

    DB_NAME = f"vitacare_historic_{CNES_CODE}"
    TABLES_TO_EXTRACT = vitacare_constants.TABLES_TO_EXTRACT.value

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            dataset=DATASET_ID,
            table=f"vitacare_all_tables_{CNES_CODE}",
        )

    # Obtem segredos e conecta ao banco
    db_credentials = get_sql_server_credentials(
        infisical_path=vitacare_constants.INFISICAL_PATH.value,
        host_key=vitacare_constants.INFISICAL_HOST.value,
        port_key=vitacare_constants.INFISICAL_PORT.value,
        user_key=vitacare_constants.INFISICAL_USERNAME.value,
        password_key=vitacare_constants.INFISICAL_PASSWORD.value,
        environment=ENVIRONMENT,
    )

    engine = get_sql_server_engine(
        db_host=db_credentials["host"],
        db_port=db_credentials["port"],
        db_user=db_credentials["user"],
        db_password=db_credentials["password"],
        db_name=DB_NAME,
    )

    # Loop de extração/carga por tabela
    for table in TABLES_TO_EXTRACT:
        bq_table_name = build_bq_table_name_vitacare(table)

        queries = create_extraction_batches_sql_server(
            engine=engine,
            db_schema=SCHEMA,
            db_table=table,
            id_column=ID_COLUMN,
        )

        dataframes = download_from_sql_server_db.map(
            engine=unmapped(engine),
            query=queries,
            cnes_code=unmapped(CNES_CODE),
        )

        upload_df_to_datalake.map(
            df=dataframes,
            dataset_id=unmapped(DATASET_ID),
            table_id=unmapped(bq_table_name),
            partition_column=unmapped(PARTITION_COLUMN),
            source_format=unmapped("parquet"),
            if_exists=unmapped("append"),
            if_storage_data_exists=unmapped("append"),
        )

# Configurações de execução
flow_vitacare_backup_mensal.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_backup_mensal.executor = LocalDaskExecutor(num_workers=10)
flow_vitacare_backup_mensal.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="5Gi",
    memory_request="5Gi",
)
flow_vitacare_backup_mensal.schedule = vitacare_monthly_schedule