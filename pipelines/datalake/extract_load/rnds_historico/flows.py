# -*- coding: utf-8 -*-
# pylint: disable=C0103

from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants as global_constants
from pipelines.datalake.extract_load.rnds_historico.constants import (
    rnds_constants,
)

from pipelines.datalake.extract_load.rnds_historico.schedules import (
    rnds_historico_schedule
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key

from pipelines.datalake.extract_load.rnds_historico.tasks import (
    process_rnds_table,
    start_rnds_instance,
    stop_rnds_instance,
)


with Flow(
    "Datalake - Extração e Carga de Dados - RNDS Historico (Cloud SQL)",
    state_handlers=[handle_flow_state_change],
    owners=[global_constants.DANIEL_ID.value],
) as flow_rnds_historico:

    ENVIRONMENT = Parameter("environment", default="dev", required=True)
    DB_SCHEMA = Parameter(
        "db_schema", default=rnds_constants.DB_SCHEMA.value
    )
    PARTITION_COLUMN = Parameter(
        "partition_column",
        default=rnds_constants.BQ_PARTITION_COLUMN.value,
    )

    DATASET_ID = rnds_constants.DATASET_ID.value
    SECRET_PATH = rnds_constants.INFISICAL_PATH.value

    db_host = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=rnds_constants.INFISICAL_HOST.value,
        environment=ENVIRONMENT,
    )
    db_port = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=rnds_constants.INFISICAL_PORT.value,
        environment=ENVIRONMENT,
    )
    db_user = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=rnds_constants.INFISICAL_USERNAME.value,
        environment=ENVIRONMENT,
    )
    db_password = get_secret_key(
        secret_path=SECRET_PATH,
        secret_name=rnds_constants.INFISICAL_PASSWORD.value,
        environment=ENVIRONMENT,
    )

    INSTANCE_NAME = "vitacare"

    start_instance = start_rnds_instance(instance_name=INSTANCE_NAME)

    tasks = []

    for ap in rnds_constants.AP_LIST.value:
        t = process_rnds_table(
            db_host=db_host,
            db_port=db_port,
            db_user=db_user,
            db_password=db_password,
            db_schema=DB_SCHEMA,
            ap=ap,
            dataset_id=DATASET_ID,
            partition_column=PARTITION_COLUMN,
            upstream_tasks=[start_instance],
        )
        tasks.append(t)

    stop_rnds_instance(instance_name=INSTANCE_NAME, upstream_tasks=tasks)


flow_rnds_historico.storage = GCS(global_constants.GCS_FLOWS_BUCKET.value)
flow_rnds_historico.executor = LocalDaskExecutor(num_workers=3)
flow_rnds_historico.run_config = KubernetesRun(
    image=global_constants.DOCKER_IMAGE.value,
    labels=[global_constants.RJ_SMS_AGENT_LABEL.value],
    memory_limit="4Gi",
    memory_request="4Gi",
)

flow_rnds_historico.schedule = rnds_historico_schedule