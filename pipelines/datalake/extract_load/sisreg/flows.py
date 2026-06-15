# -*- coding: utf-8 -*-
"""
Fluxo unificado SISREG - unico ponto de entrada para todos os conjuntos.

DAG:
  resolver_credenciais
    -> planejar_trabalho
      -> extrair_item (chamada direta - single-flight por conta)
        -> consolidar (trigger=all_finished)
          -> normalizar_e_subir
            -> registrar_log_execucao (trigger=all_finished)

O fluxo e parametrizado: dataset_id e table_ids sao Parameters com defaults
do registry/constants para facilitar renomear sem alterar codigo. O conjunto
(dataset key) tambem e um Parameter - cada clock de schedule passa o seu.

Single-flight (R1): SISREG permite apenas uma sessao ativa por conta. O uso
de num_workers=1 garante requisicoes seriais. planejar_trabalho retorna um
unico item; o loop sobre sub-itens (CPFs, roteiro, procedimentos) e feito
dentro de extrair_item com a sessao reusada - 1 login por run.
"""

from prefect import Parameter
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GCS

from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.datalake.extract_load.sisreg.tasks import (
    consolidar,
    extrair_item,
    normalizar_e_subir,
    obter_data_extracao,
    planejar_trabalho,
    registrar_log_execucao,
    resolver_credenciais,
)
from pipelines.utils.flow import Flow
from pipelines.utils.state_handlers import handle_flow_state_change

# ---------------------------------------------------------------------------
# Parametros do fluxo
# ---------------------------------------------------------------------------

with Flow(
    name="SUBGERAL/CR/NTI - Extract & Load - SISREG Web Unificado",
    state_handlers=[handle_flow_state_change],
    owners=[pipeline_constants.MATHEUS_ID.value],
) as sisreg_web_flow:

    # Ambiente: "staging" ou "prod". Obrigatorio (authenticated_task asserta).
    ENVIRONMENT = Parameter("environment", default="staging", required=True)

    # Chave do conjunto de dados a extrair nesta execucao.
    # Ex.: "escalas", "afastamentos", "preparos", "solicitacoes", "fila_vagas"
    CONJUNTO = Parameter("conjunto", default="escalas", required=True)

    # Dataset e tabelas de destino - parametrizados para renomear sem alterar codigo.
    DATASET_ID = Parameter("dataset_id", default=C.DATASET_ID_PADRAO, required=False)

    # Parametros extras passados a planejar_trabalho/extrair_item do conjunto.
    # Contem: data_inicial, data_final (window), limite de debug, etc.
    JANELA_DIAS = Parameter("janela_dias", default=C.JANELA_DIAS, required=False)

    # ---------------------------------------------------------------------------
    # DAG
    # ---------------------------------------------------------------------------

    credenciais = resolver_credenciais(
        conjunto=CONJUNTO,
        environment=ENVIRONMENT,
    )

    # Data de extracao computada em runtime (fuso SP) - usada como particao.
    # Task dedicada garante consistencia entre consolidar e o log de execucao.
    data_extracao = obter_data_extracao()

    params = {"janela_dias": JANELA_DIAS, "environment": ENVIRONMENT}

    item = planejar_trabalho(
        conjunto=CONJUNTO,
        credenciais=credenciais,
        params=params,
        upstream_tasks=[credenciais],
    )

    # extrair_item executado diretamente (sem .map).
    # O loop sobre sub-itens (CPFs, roteiro, procedimentos) ocorre dentro do
    # extrator com a sessao reusada: 1 login por run (anti-ban, R1).
    fragmento = extrair_item(
        conjunto=CONJUNTO,
        item=item,
        credenciais=credenciais,
        params=params,
    )

    # consolidar usa trigger=all_finished: roda mesmo se a task upstream falhou.
    # Retorna Consolidado com tabelas=None quando o gate de completude falha.
    consolidado = consolidar(
        conjunto=CONJUNTO,
        fragmento=fragmento,
        data_extracao=data_extracao,
        upstream_tasks=[fragmento],
    )

    subiu = normalizar_e_subir(
        environment=ENVIRONMENT,
        conjunto=CONJUNTO,
        dataset_id=DATASET_ID,
        consolidado=consolidado,
        upstream_tasks=[consolidado],
    )

    registrar_log_execucao(
        conjunto=CONJUNTO,
        consolidado=consolidado,
        subiu=subiu,
        dataset_id=DATASET_ID,
        upstream_tasks=[subiu],
    )

# ---------------------------------------------------------------------------
# Infraestrutura: executor, storage, run config
# ---------------------------------------------------------------------------

# num_workers=1: single-flight dentro de uma execucao.
from pipelines.datalake.extract_load.sisreg.schedules import (  # noqa: E402
    schedule as sisreg_schedule,
)

sisreg_web_flow.schedule = sisreg_schedule
sisreg_web_flow.executor = LocalDaskExecutor(num_workers=1)

sisreg_web_flow.storage = GCS(pipeline_constants.GCS_FLOWS_BUCKET.value)

sisreg_web_flow.run_config = VertexRun(
    image=pipeline_constants.DOCKER_VERTEX_IMAGE.value,
    labels=[pipeline_constants.RJ_SMS_VERTEX_AGENT_LABEL.value],
    # e2-standard-4: 4 vCPU, 16 GB - adequado para scrapers de longa duracao
    machine_type="e2-standard-4",
    env={
        "INFISICAL_ADDRESS": pipeline_constants.INFISICAL_ADDRESS.value,
        "INFISICAL_TOKEN": pipeline_constants.INFISICAL_TOKEN.value,
    },
)
