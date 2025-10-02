# -*- coding: utf-8 -*-
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.reports.alerta_atualizacao_tabelas.schedules import schedule
from pipelines.reports.alerta_atualizacao_tabelas.tasks import (
    verify_tables_freshness,
    send_discord_alert,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

with Flow(
    "Report: Alerta Atualização Tabelas",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.PEDRO_ID.value,
    ],
) as report_alerta_atualizacao_tabelas:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)
    TABLE_IDS = Parameter("table_ids", default={})

    results = verify_tables_freshness(environment=ENVIRONMENT, table_ids=TABLE_IDS)

    send_discord_alert(environment=ENVIRONMENT, results=results)

report_alerta_atualizacao_tabelas.executor = LocalDaskExecutor(num_workers=1)
report_alerta_atualizacao_tabelas.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
report_alerta_atualizacao_tabelas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
)
report_alerta_atualizacao_tabelas.schedule = schedule
