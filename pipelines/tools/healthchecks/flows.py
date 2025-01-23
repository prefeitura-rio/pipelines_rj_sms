# -*- coding: utf-8 -*-
from prefect import Parameter, case, unmapped
from prefect.tasks.control_flow import merge
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.tools.healthchecks.schedules import schedule
from pipelines.tools.healthchecks.tasks import (
    vitai_db_health_check,
    smsrio_db_health_check,
    vitacare_api_health_check,
    print_result,
    transform_to_df,
)
from pipelines.utils.tasks import upload_df_to_datalake


with Flow("Tool: Health Check") as flow:

    ENVIRONMENT = Parameter("environment", default="dev")

    result_vitai_db = vitai_db_health_check(enviroment=ENVIRONMENT)
    result_smsrio_db = smsrio_db_health_check(enviroment=ENVIRONMENT)
    result_vitacare_api = vitacare_api_health_check(enviroment=ENVIRONMENT)

    results = merge(
        result_smsrio_db,
        result_vitacare_api,
        result_vitai_db
    )

    print_result(
        results=results,
        enviroment=ENVIRONMENT
    )

    results_as_df = transform_to_df(
        results=results,
    )

    upload_df_to_datalake(
        df = results_as_df,
        partition_column="created_at",
        dataset_id="brutos_monitoramento",
        table_id="healthchecks",
        source_format="parquet",
    )
    

flow.executor = LocalDaskExecutor(num_workers=3)
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
flow.schedule = schedule
