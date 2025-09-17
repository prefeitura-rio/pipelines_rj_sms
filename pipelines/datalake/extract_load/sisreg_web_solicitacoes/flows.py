# -*- coding: utf-8 -*-
"""
Fluxo
"""

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datalake.extract_load.sisreg_web_solicitacoes.schedules import schedule
from pipelines.datalake.extract_load.sisreg_web_solicitacoes.tasks import full_extract_process
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change
from pipelines.utils.tasks import get_secret_key

with Flow(
    "SUBGERAL - Extract & Load - SISREG Solicitações (Web)",
    state_handlers=[handle_flow_state_change],
    owners=[
        constants.MATHEUS_ID.value,
    ],
) as sisreg_web_sol:
    # Parâmetros -----------------------------------------------------------
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # Parâmetros da consulta --------------------------------
    DT_INICIAL = Parameter("dt_inicial", default="02/06/2025", required=True)
    DT_FINAL = Parameter("dt_final", default="02/06/2025", required=True)
    TIPO_PERIODO = Parameter("tipo_periodo", default="S", required=True) # 'S' = SOLICITAÇÃO
    SITUACAO = Parameter("situacao", default="1", required=True) # '1' = SOL/PEN/REG
    PAGE_SIZE = Parameter("page_size", default=0, required=True)  # 10,20,50,100 ou 0 = TODOS
    ORDENACAO = Parameter("ordenacao", default="2", required=True)
    MAX_PAGES = Parameter("max_pages", default=None, required=False) # None = até o fim
    WAIT_SECONDS = Parameter("wait", default=0.5, required=True) # tempo entre requisições de páginas

    # Credenciais ----------------------------------------------------------
    user = get_secret_key(
        environment=ENVIRONMENT, secret_name="SISREG_USER", secret_path="/sisreg"
    )
    password = get_secret_key(
        environment=ENVIRONMENT, secret_name="SISREG_PASSWORD", secret_path="/sisreg"
    )

    # Tarefas -------------------------------------
    full_extract_process(usuario=user,
                         senha=password,
                         dt_inicial=DT_INICIAL,
                         dt_final=DT_FINAL,
                         situacao=SITUACAO,
                         tipo_periodo=TIPO_PERIODO,
                         page_size=PAGE_SIZE,
                         ordenacao=ORDENACAO,
                         max_pages=MAX_PAGES,
                         wait=WAIT_SECONDS
    )

sisreg_web_sol.executor = LocalDaskExecutor(num_workers=10)
sisreg_web_sol.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
sisreg_web_sol.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_SMS_AGENT_LABEL.value],
    memory_request="13Gi",
    memory_limit="13Gi",
)
sisreg_web_sol.schedule = schedule
