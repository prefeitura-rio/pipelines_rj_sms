# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.tools.healthchecks.schedules import schedule
from pipelines.tools.healthchecks.tasks import (
    smsrio_db_health_check,
    transform_to_df,
    vitai_api_health_check,
    vitai_db_health_check,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import upload_df_to_datalake

with Flow(
    "Tool: Health Check",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.DIT_ID.value,
    ],
) as flow_healthcheck:

    ENVIRONMENT = Parameter("environment", default="dev")

    result_vitai_db = vitai_db_health_check(enviroment=ENVIRONMENT)
    result_smsrio_db = smsrio_db_health_check(enviroment=ENVIRONMENT)
    result_vitai_api = vitai_api_health_check(enviroment=ENVIRONMENT)

    results_as_df = transform_to_df(
        results_smsrio=result_smsrio_db,
        results_vitai_db=result_vitai_db,
        results_vitai_api=result_vitai_api,
    )

    upload_df_to_datalake(
        df=results_as_df,
        partition_column="created_at",
        dataset_id="brutos_monitoramento",
        table_id="healthchecks",
        source_format="parquet",
    )


flow_healthcheck.executor = LocalDaskExecutor(num_workers=8)
flow_healthcheck.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow_healthcheck.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
flow_healthcheck.schedule = schedule
