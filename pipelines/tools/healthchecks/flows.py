# -*- coding: utf-8 -*-
from prefect import Parameter, case, flatten, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.control_flow import merge
from prefeitura_rio.pipelines_utils.custom import Flow

from pipelines.constants import constants
from pipelines.datalake.extract_load.vitacare_api.constants import (
    constants as vitacare_constants,
)
from pipelines.tools.healthchecks.schedules import schedule
from pipelines.tools.healthchecks.tasks import (
    get_ap_list,
    print_result,
    smsrio_db_health_check,
    transform_to_df,
    vitacare_api_health_check,
    vitai_db_health_check,
)
from pipelines.utils.tasks import upload_df_to_datalake

with Flow("Tool: Health Check") as flow:

    ENVIRONMENT = Parameter("environment", default="dev")

    result_vitai_db = vitai_db_health_check(enviroment=ENVIRONMENT)
    result_smsrio_db = smsrio_db_health_check(enviroment=ENVIRONMENT)

    ap_list = get_ap_list()

    results_vitacare_api = vitacare_api_health_check.map(
        enviroment=unmapped(ENVIRONMENT),
        ap=ap_list,
    )

    results_vitacare_api_flattened = flatten(results_vitacare_api)

    results_as_df = transform_to_df(
        results_smsrio=result_smsrio_db,
        results_vitai=result_vitai_db,
        results_vitacare=results_vitacare_api_flattened,
    )

    upload_df_to_datalake(
        df=results_as_df,
        partition_column="created_at",
        dataset_id="brutos_monitoramento",
        table_id="healthchecks",
        source_format="parquet",
    )


flow.executor = LocalDaskExecutor(num_workers=8)
flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
flow.schedule = schedule
