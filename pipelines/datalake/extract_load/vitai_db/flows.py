# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitai_db.schedules import (
    vitai_db_extraction_schedule,
)
from pipelines.datalake.extract_load.vitai_db.tasks import (
    get_bigquery_project_from_environment,
    get_last_timestamp_from_tables,
    import_vitai_table_to_csv,
    list_tables_to_import,
)
from pipelines.prontuarios.utils.tasks import get_project_name, rename_current_flow_run
from pipelines.utils.tasks import get_secret_key, upload_to_datalake

with Flow(
    name="Datalake - Extração e Carga de Dados - Vitai (Rio Saúde)",
) as sms_dump_vitai_rio_saude:
    #####################################
    # Tasks section #1 - Setup Environment
    #####################################
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW = Parameter("rename_flow", default=False)

    with case(RENAME_FLOW, True):
        rename_current_flow_run(environment=ENVIRONMENT)

    #####################################
    # Tasks section #2 - Extraction Preparation
    #####################################
    db_url = get_secret_key(
        environment=ENVIRONMENT, secret_name="DB_URL", secret_path="/prontuario-vitai"
    )

    project_name = get_project_name(environment=ENVIRONMENT)

    tables_to_import = list_tables_to_import()

    bigquery_project = get_bigquery_project_from_environment(environment=ENVIRONMENT)

    most_recent_timestamp_per_table = get_last_timestamp_from_tables(
        project_name=bigquery_project,
        dataset_name="vitai_db",
        table_names=tables_to_import,
        column_name="datahora",
    )

    #####################################
    # Tasks section #3 - Downloading Table Data
    #####################################
    file_names = import_vitai_table_to_csv.map(
        db_url=unmapped(db_url),
        table_name=tables_to_import,
        interval_start=most_recent_timestamp_per_table,
    )

    #####################################
    # Tasks section #2 - Uploading to Datalake
    #####################################
    upload_to_datalake_task = upload_to_datalake.map(
        input_path=file_names,
        dataset_id=unmapped("vitai_db"),
        table_id=tables_to_import,
        if_exists=unmapped("replace"),
        source_format=unmapped("csv"),
        if_storage_data_exists=unmapped("replace"),
        biglake_table=unmapped(True),
        dataset_is_public=unmapped(False),
    )

sms_dump_vitai_rio_saude.schedule = vitai_db_extraction_schedule
sms_dump_vitai_rio_saude.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitai_rio_saude.executor = LocalDaskExecutor(num_workers=1)
sms_dump_vitai_rio_saude.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="5Gi",
)
