# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.reports.alerta_jobs_caros.schedules import schedule
from pipelines.reports.alerta_jobs_caros.tasks import (
    get_recent_bigquery_jobs,
    send_discord_alert,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    "Report: Alerta Jobs Caros",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.PEDRO_ID.value,
    ],
) as report_alerta_jobs_caros:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    jobs = get_recent_bigquery_jobs(environment=ENVIRONMENT, cost_threshold=0.5, time_threshold=24)

    send_discord_alert(environment=ENVIRONMENT, results=jobs)

report_alerta_jobs_caros.executor = LocalDaskExecutor(num_workers=1)
report_alerta_jobs_caros.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_alerta_jobs_caros.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
report_alerta_jobs_caros.schedule = schedule
