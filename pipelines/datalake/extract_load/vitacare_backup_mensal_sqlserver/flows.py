# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flow de extração e carga de dados do Vitacare Historic SQL Server para o BigQuery
"""

from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.constants import (
    constants as vitacare_constants,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.schedules import (
    vitacare_monthly_schedule,
)
from pipelines.datalake.extract_load.vitacare_backup_mensal_sqlserver.tasks import (
    build_bq_table_name,
    extract_and_transform_table,
    generate_extraction_cartesian_product,
    get_all_cnes_codes,
    get_tables_to_extract,
)
from pipelines.datalake.utils.tasks import rename_current_flow_run
from pipelines.utils.tasks import get_secret_key, upload_df_to_datalake

with Flow("DataLake - Extração e Carga - Vitacare Historic") as flow_vitacare_historic:
    # Parâmetros do Flow
    ENVIRONMENT = Parameter("environment", default="staging")
    SCHEMA = Parameter("schema", default="dbo")
    PARTITION_COLUMN = Parameter("partition_column", default="extracted_at")
    RENAME_FLOW = Parameter("rename_flow", default=False)

    DATASET_ID = vitacare_constants.DATASET_ID.value
    TABLES_TO_EXTRACT = get_tables_to_extract()
    ALL_CNES_CODES = get_all_cnes_codes()

    cnes_to_process_list, tables_to_process_list = (
        generate_extraction_cartesian_product(  # <<< ADICIONAR CHAMADA
            cnes_codes=ALL_CNES_CODES, tables_to_extract=TABLES_TO_EXTRACT
        )
    )

    with case(RENAME_FLOW, True):
        rename_current_flow_run(
            environment=ENVIRONMENT,
            dataset=DATASET_ID,
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
    connection_name = get_secret_key(
        secret_path=vitacare_constants.INFISICAL_PATH.value,
        secret_name=vitacare_constants.INFISICAL_CONNECTION_NAME.value,
        environment=ENVIRONMENT,
    )

    extracted_dfs = extract_and_transform_table.map(
        db_host=unmapped(host),
        db_port=unmapped(port),
        db_user=unmapped(user),
        db_password=unmapped(password),
        db_schema=unmapped(SCHEMA),
        db_table=tables_to_process_list,
        cnes_code=cnes_to_process_list,
    )

    bq_table_id_futures = build_bq_table_name.map(table_name=TABLES_TO_EXTRACT)

    upload_df_to_datalake.map(
        df=extracted_dfs,
        dataset_id=unmapped(DATASET_ID),
        table_id=bq_table_id_futures,
        partition_column=unmapped(PARTITION_COLUMN),
        source_format=unmapped("parquet"),
        if_exists=unmapped("append"),
        if_storage_data_exists=unmapped("append"),
    )

flow_vitacare_historic.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_vitacare_historic.executor = LocalDaskExecutor(num_workers=1)
flow_vitacare_historic.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="8Gi",
    memory_request="5Gi",
)
flow_vitacare_historic.schedule = vitacare_monthly_schedule
