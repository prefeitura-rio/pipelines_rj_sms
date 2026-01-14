# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa E501
from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_gdrive.schedules import schedule
from pipelines.datalake.extract_load.vitacare_gdrive.tasks import (
    find_all_file_names_from_pattern,
    get_most_recent_schema,
    report_inadequacy,
    upload_consistent_files,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    name="DataLake - Extração e Carga de Dados - Vitacare GDrive",
    state_handlers=[handle_flow_state_change],
    owners=[constants.DIT_ID.value],
) as sms_dump_vitacare_reports:
    #####################################
    # Parameters
    #####################################

    # Flow
    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    RENAME_FLOW_RUN = Parameter("rename_flow_run", default=False, required=False)
    FILE_PATTERN = Parameter("file_pattern", default=False, required=False)
    DESIRED_DATASET_NAME = Parameter("desired_dataset_name", default="brutos_prontuario_vitacare")
    DESIRED_TABLE_NAME = Parameter("desired_table_name", default=None)

    file_names = find_all_file_names_from_pattern(
        file_pattern=FILE_PATTERN,
        environment=ENVIRONMENT,
    )

    most_recent_schema = get_most_recent_schema(
        file_pattern=FILE_PATTERN,
        environment=ENVIRONMENT,
    )

    reports = upload_consistent_files.map(
        file_name=file_names,
        expected_schema=unmapped(most_recent_schema),
        environment=unmapped(ENVIRONMENT),
        dataset_id=unmapped(DESIRED_DATASET_NAME),
        table_id=unmapped(DESIRED_TABLE_NAME),
        use_safe_download_file=unmapped(False),
    )

    report_inadequacy(
        file_pattern=FILE_PATTERN,
        reports=reports,
    )

# Storage and run configs
sms_dump_vitacare_reports.schedule = schedule
sms_dump_vitacare_reports.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sms_dump_vitacare_reports.executor = LocalDaskExecutor(num_workers=1)
sms_dump_vitacare_reports.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_limit="13Gi",
    memory_request="13Gi",
)
