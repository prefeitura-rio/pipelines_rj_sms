# -*- coding: utf-8 -*-
"""
Fluxo independente de monitoramento de frescor do SISREG.

Executa em schedule proprio (diario, 08:00 SP) e verifica se cada conjunto
teve execucao bem-sucedida dentro do SLA definido em constants.SLA_FRESCOR_DIAS.

Por que fluxo proprio e nao uma task no fluxo de extracao:
  O monitor existe para detectar quando o fluxo de extracao PAROU de executar
  (agente inativo, schedule cancelado, lock eterno). Se o monitor rodasse
  dentro do mesmo fluxo, ele nunca seria acionado nesses cenarios - o proprio
  fluxo que o aciona nao rodaria. O monitor deve ser independente.
"""

from datetime import datetime, timedelta

import pytz
from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
from prefect.schedules import Schedule
from prefect.storage import GCS

from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.datalake.extract_load.sisreg.monitor import verificar_frescor_conjuntos
from pipelines.utils.flow import Flow
from pipelines.utils.schedules import generate_dump_api_schedules, untuple_clocks
from pipelines.utils.state_handlers import handle_flow_state_change

_FUSO_SP = pytz.timezone("America/Sao_Paulo")

# Dispara diariamente as 08:00 SP - apos a janela nocturna de extracao.
_INICIO_MONITOR = datetime(2025, 1, 1, 8, 0, tzinfo=_FUSO_SP)

_CLOCKS_MONITOR = generate_dump_api_schedules(
    interval=timedelta(days=1),
    start_date=_INICIO_MONITOR,
    labels=[pipeline_constants.RJ_SMS_AGENT_LABEL.value],
    flow_run_parameters=[
        {"environment": "prod", "dataset_id": C.DATASET_ID_PADRAO},
    ],
    runs_interval_minutes=2,
)

_SCHEDULE_MONITOR = Schedule(clocks=untuple_clocks(_CLOCKS_MONITOR))

# ---------------------------------------------------------------------------
# Fluxo do monitor de frescor
# ---------------------------------------------------------------------------

with Flow(
    name="SUBGERAL/CR/NTI - Monitor - SISREG Frescor",
    state_handlers=[handle_flow_state_change],
    owners=[pipeline_constants.MATHEUS_ID.value],
) as sisreg_monitor_flow:
    ENVIRONMENT = Parameter("environment", default="staging", required=True)
    DATASET_ID = Parameter("dataset_id", default=C.DATASET_ID_PADRAO, required=False)

    verificar_frescor_conjuntos(
        dataset_id=DATASET_ID,
        environment=ENVIRONMENT,
    )

sisreg_monitor_flow.schedule = _SCHEDULE_MONITOR
sisreg_monitor_flow.executor = LocalDaskExecutor(num_workers=1)
sisreg_monitor_flow.storage = GCS(pipeline_constants.GCS_FLOWS_BUCKET.value)

# e2-standard-4: padrao do repo (todos os flows Vertex usam este tipo).
# O monitor e leve (uma leitura de BQ), mas mantemos o tipo padrao por
# consistencia e para nao introduzir um valor de infra nao testado.
sisreg_monitor_flow.run_config = VertexRun(
    image=pipeline_constants.DOCKER_VERTEX_IMAGE.value,
    labels=[pipeline_constants.RJ_SMS_VERTEX_AGENT_LABEL.value],
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": pipeline_constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": pipeline_constants.INFISICAL_TOKEN.value,
    },
)
