# -*- coding: utf-8 -*-
"""
Fluxo unificado SISREG - unico ponto de entrada para todos os conjuntos.

DAG map-reduce:
  resolver_credenciais
    -> planejar_trabalho
      -> extrair_item.map (num_workers=1 - single-flight por conta)
        -> consolidar (trigger=all_finished)
          -> normalizar_e_subir
            -> registrar_log_execucao (trigger=all_finished)

O fluxo e parametrizado: dataset_id e table_ids sao Parameters com defaults
do registry/constants para facilitar renomear sem alterar codigo. O conjunto
(dataset key) tambem e um Parameter - cada clock de schedule passa o seu.

Single-flight (R1): SISREG permite apenas uma sessao ativa por conta. O uso
de num_workers=1 garante requisicoes seriais dentro de uma execucao. O soft-lock
por conta (GCS TTL) impede duas execucoes do mesmo conjunto rodarem ao mesmo
tempo - veja _adquirir_lock / _liberar_lock abaixo.
"""

from prefect import Parameter, unmapped
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import VertexRun
from prefect.storage import GCS

from pipelines.constants import constants as pipeline_constants
from pipelines.datalake.extract_load.sisreg import constants as C
from pipelines.datalake.extract_load.sisreg.tasks import (
    consolidar,
    extrair_item,
    normalizar_e_subir,
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

    # Jitter de inicio: atraso aleatorio para evitar periocidade perfeita nos
    # firings do schedule (generate_dump_api_schedules nao tem jitter nativo).
    # Em producao o schedule ja staggers os clocks; o jitter e uma camada extra.
    JITTER_MAXIMO_S = Parameter("jitter_maximo_s", default=30, required=False)

    # ---------------------------------------------------------------------------
    # DAG
    # ---------------------------------------------------------------------------

    credenciais = resolver_credenciais(
        conjunto=CONJUNTO,
        environment=ENVIRONMENT,
    )

    # Data de extracao em formato YYYY-MM-DD (fuso SP) - usada como particao.
    # Obtida aqui uma vez e passada para consolidar/log para consistencia.
    DATA_EXTRACAO = Parameter("data_extracao", default="", required=False)

    params = {"janela_dias": JANELA_DIAS, "environment": ENVIRONMENT}

    items = planejar_trabalho(
        conjunto=CONJUNTO,
        credenciais=credenciais,
        params=params,
        upstream_tasks=[credenciais],
    )

    # .map executa extrair_item para cada item da lista.
    # num_workers=1 garante execucao serial - nunca duas requisicoes simultaneas
    # na mesma conta (single-flight per account, R1 do design).
    fragmentos = extrair_item.map(
        conjunto=unmapped(CONJUNTO),
        item=items,
        credenciais=unmapped(credenciais),
        params=unmapped(params),
    )

    # consolidar usa trigger=all_finished: roda mesmo se alguns itens falharam,
    # mas o gate de completude interno cancela o upload se houver falhas.
    tabelas = consolidar(
        conjunto=CONJUNTO,
        fragmentos_lista=fragmentos,
        data_extracao=DATA_EXTRACAO,
        upstream_tasks=[fragmentos],
    )

    subiu = normalizar_e_subir(
        environment=ENVIRONMENT,
        conjunto=CONJUNTO,
        dataset_id=DATASET_ID,
        tabelas_consolidadas=tabelas,
        upstream_tasks=[tabelas],
    )

    registrar_log_execucao(
        conjunto=CONJUNTO,
        items_total=0,  # preenchido dentro da task via len(fragmentos)
        items_ok=0,
        linhas_por_tabela=None,
        status="OK",
        dataset_id=DATASET_ID,
        upstream_tasks=[subiu],
    )

# ---------------------------------------------------------------------------
# Infraestrutura: executor, storage, run config
# ---------------------------------------------------------------------------

# num_workers=1: single-flight dentro de uma execucao.
# .map ainda oferece isolamento de retry por item - o beneficio real de usar .map.
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
