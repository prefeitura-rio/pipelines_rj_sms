# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.prontuarios.utils.tasks import get_datetime_working_range
from pipelines.tools.vitacare_healthcheck.constants import (
    constants as vitacare_constants,
)
from pipelines.tools.vitacare_healthcheck.schedules import schedule
from pipelines.tools.vitacare_healthcheck.tasks import (
    filter_files_by_date,
    fix_column_typing,
    get_file_content,
    get_files_from_folder,
    get_structured_files_metadata,
    json_records_to_dataframe,
    loading_data_to_bigquery,
)

with Flow(
    name="Tool: Monitoramento de Conectividade (Vitacare)",
) as monitoramento:
    ENVIRONMENT = Parameter("environment", default="dev")
    START_DATE = Parameter("start_date", default="")
    END_DATE = Parameter("end_date", default="")

    file_list = get_files_from_folder(folder_id=vitacare_constants.TARGET_FOLDER_ID.value)

    start_datetime, end_datetime = get_datetime_working_range(
        start_datetime=START_DATE, end_datetime=END_DATE
    )

    structured_files_metadata = get_structured_files_metadata(file_list=file_list)

    day_files = filter_files_by_date(
        files=structured_files_metadata, start_datetime=start_datetime, end_datetime=end_datetime
    )

    files = get_file_content.map(file_metadata=day_files)

    files_as_df = json_records_to_dataframe(json_records=files)

    files_fixed = fix_column_typing(dataframe=files_as_df)

    loading_data_to_bigquery(data=files_fixed, environment=ENVIRONMENT)

monitoramento.schedule = schedule
monitoramento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
monitoramento.executor = LocalDaskExecutor(num_workers=10)
monitoramento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[
        constants.RJ_SMS_AGENT_LABEL.value,
    ],
    memory_request="2Gi",
    memory_limit="2Gi",
)
